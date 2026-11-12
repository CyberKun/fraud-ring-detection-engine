"""Run against docker compose up. Requires Python Playwright and Chromium."""
import argparse
import json
import re
import subprocess
import time
import urllib.parse
import urllib.request
from playwright.sync_api import sync_playwright, expect


def wait(check, seconds=60):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        try:
            result = check()
            if result:
                return result
        except OSError:
            pass
        time.sleep(.4)
    raise AssertionError("Timed out waiting for " + repr(check))


def main(url, failover, screenshot, canvas_limit):
    def get(path):
        with urllib.request.urlopen(url+path, timeout=12) as response:
            return json.load(response)

    def healthy():
        h = get("/api/cluster/health")["shards"]
        return h if len(h)==4 and all(s["replica_role"]=="LEADER" and
            sorted(m["replica_role"] for m in s["members"])==["FOLLOWER","FOLLOWER","LEADER"] for s in h) else None

    topology = wait(healthy)
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width":1440,"height":980})
        events, errors = [], []
        page.on("pageerror",lambda exc: errors.append(str(exc)))
        def frame(payload):
            packet=json.loads(payload)
            if packet.get("type")=="alert":
                events.append(packet["event"])
        page.on("websocket",lambda ws: ws.on("framereceived",frame) if "/api/alerts/stream" in ws.url else None)
        page.goto(url)
        seed=page.get_by_role("button",name="Seed demo ring")
        expect(seed).to_be_enabled(timeout=20000)
        with page.expect_response(lambda r: r.url.endswith("/api/dev/seed-demo")) as submitted:
            seed.click()
        assert submitted.value.ok, submitted.value.text()
        row=page.locator("button.row").first
        expect(row).to_be_visible(timeout=15000)
        event=events[0]
        assert event["stats"]["account_count"]=="7" and event["stats"]["flagged_accounts"]=="2"
        base=f'/api/components/{event["tenant_id"]}/{event["vertex_id"]}'
        stats=get(base+"/stats?min_lsn="+event["lsn"])
        assert stats["replica_role"] in ("LEADER","FOLLOWER") and int(stats["lag_records"])>=0
        graph=get(base+"/subgraph?max_nodes=2000&min_lsn="+event["lsn"])
        assert stats["volume"]=="16905" and len(graph["vertices"])==10 and len(graph["edges"])==20
        assert {v["entity_type"] for v in graph["vertices"]}=={"ACCOUNT","CARD","DEVICE"}
        truncated=get(base+"/subgraph?max_nodes=3&min_lsn="+event["lsn"])
        assert truncated["truncated"] and len(truncated["vertices"])==3 and truncated["total_vertices"]=="10"
        row.focus()
        page.keyboard.press("Enter")
        expect(page.get_by_role("heading",name="Component explorer")).to_be_visible()
        canvas=page.locator("canvas")
        expect(canvas).to_have_attribute("data-vertices","10")
        page.wait_for_function("""() => {
          const c=document.querySelector('canvas');if(!c)return false;
          const d=c.getContext('2d').getImageData(0,0,c.width,c.height).data;
          const wanted=new Set(['79,140,255','167,139,250','45,212,191','240,68,56']);
          for(let i=0;i<d.length;i+=4)if(d[i+3]===255)wanted.delete(d.slice(i,i+3).join(','));
          return !wanted.size;
        }""")
        if screenshot:page.screenshot(path=screenshot,full_page=True)
        page.get_by_role("button",name="Audit time travel").click()
        expect(page.locator("canvas")).to_have_attribute("data-vertices","10")
        expect(page.get_by_text("First alert",exact=False)).to_be_visible()
        historical=get(f'/api/audit/{event["tenant_id"]}/{event["vertex_id"]}?timestamp_ms={event["event_time_ms"]}&min_lsn={event["lsn"]}')
        assert historical["first_alert_timestamp_ms"]==event["event_time_ms"]
        assert historical["stats"]["volume"]==stats["volume"]
        slider=page.get_by_role("slider")
        slider.focus()
        with page.expect_response(lambda r:"/api/audit/" in r.url and r.status==404):
            slider.press("Home")
        slider.press("End")
        expect(page.locator("canvas")).to_have_attribute("data-vertices","10")
        page.get_by_role("button",name="Cluster health").click()
        expect(page.locator(".shard")).to_have_count(4)
        expect(page.locator(".shard tbody tr")).to_have_count(12)
        panel=page.frame_locator("iframe").locator("body")
        expect(panel).to_contain_text("WAL fsync",timeout=30000)
        expr=urllib.parse.quote('count(up{job="fraud-replicas"} == 1)')
        wait(lambda: any(float(r["value"][1])==12 for r in get("/grafana/api/datasources/proxy/uid/fraud-prom/api/v1/query?query="+expr)["data"]["result"]))
        print("websocket_alert=passed accounts=7 flagged=2 volume=16905")
        print("explorer_canvas=10_vertices 20_edges entity_colors=3 flagged_ring=passed truncation=3_of_10")
        print("audit_slider=passed first_crossing=exact shards=4 replicas=12 prometheus_targets=12 grafana_panel=passed")
        if failover:
            shard=topology[0]
            service=next(m["address"].split(":")[0] for m in shard["members"] if m["replica_role"]=="LEADER")
            assert re.fullmatch("s[0-3]r[0-2]",service)
            card=page.locator(".shard").filter(has=page.get_by_role("heading",name=f'Shard {shard["shard_id"]}',exact=True))
            card.get_by_role("button",name="Kill leader").click()
            try:
                with page.expect_response(lambda r:"/api/dev/kill-leader/" in r.url) as stopped:
                    card.get_by_role("button",name="Confirm stop").click()
                assert stopped.value.ok,stopped.value.text()
                def promoted():
                    current=next(s for s in get("/api/cluster/health")["shards"] if s["shard_id"]==shard["shard_id"])
                    return current if current["replica_role"]=="LEADER" and int(current["epoch"])>int(shard["epoch"]) and any(m["replica_role"]=="UNAVAILABLE" for m in current["members"]) else None
                new=wait(promoted)
                print(f'kill_leader=passed shard={shard["shard_id"]} old_epoch={shard["epoch"]} new_epoch={new["epoch"]}')
            finally:
                subprocess.run(["docker","compose","start",service],check=True)
                wait(healthy)
        if canvas_limit:
            tenant=str(time.time_ns()//1000)
            expiry=str(int(time.time()*1000)+3600000)
            payload="".join(json.dumps({"upsert":{"tenant_id":tenant,"u":str(v-1),"v":str(v),"expiry_ms":expiry,
                "source":{"producer_id":"1","sequence":str(v)}}})+"\n" for v in range(1,2001))
            subprocess.run(["docker","compose","exec","-T","router","./build/fraud_produce",
                            "http://etcd:2379","broker:9092","fraud-mutations"],input=payload,text=True,check=True)
            wait(lambda: get(f"/api/components/{tenant}/0/stats")["vertex_count"]=="2001",180)
            page.get_by_role("button",name="Component explorer",exact=True).click()
            page.locator(".query").get_by_label("Tenant",exact=True).fill(tenant)
            page.get_by_label("Vertex ID",exact=True).fill("0")
            page.get_by_role("button",name="Inspect",exact=True).click()
            expect(page.locator("canvas")).to_have_attribute("data-vertices","2000",timeout=15000)
            expect(page.get_by_text("+1 more vertices")).to_be_visible()
            intervals=page.evaluate("""() => new Promise(resolve=>{
              let previous=0;const samples=[];
              function measure(now){if(previous)samples.push(now-previous);previous=now;
                if(samples.length===60)resolve(samples.sort((a,b)=>a-b));else requestAnimationFrame(measure);}
              requestAnimationFrame(measure);
            })""")
            print(f"canvas_cap=2000/2001 truncation_banner=passed median_frame_ms={intervals[30]:.1f} p95_frame_ms={intervals[56]:.1f}")
        assert not errors, errors
        browser.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--url",default="http://localhost:8180")
    parser.add_argument("--failover",action="store_true")
    parser.add_argument("--screenshot")
    parser.add_argument("--canvas-limit",action="store_true")
    args=parser.parse_args()
    main(args.url,args.failover,args.screenshot,args.canvas_limit)
