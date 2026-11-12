"""Isolated real etcd, router and three durable replica processes per shard."""
import base64
import concurrent.futures
import contextlib
import csv
import itertools
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

import grpc
from phase6_chaos import addr, port, until

BUILD = Path("/workspace/build")
RESULTS = Path(sys.argv[1])
clock = time.monotonic


def write_rows(name, header, rows):
    with (RESULTS / (name + ".csv")).open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header.split(","))
        writer.writerows(rows)


def percentile(samples, fraction):
    return sorted(samples)[math.ceil(len(samples) * fraction) - 1] if samples else ""


def cluster(shards, directory, cleanup, p, rpc, summaries, samples):
    log = cleanup.enter_context((directory / "service.log").open("w"))
    def launch(args):
        proc = subprocess.Popen(args, stdout=log, stderr=log)
        def stop():
            if proc.poll() is None:
                proc.kill()
            proc.wait()
        cleanup.callback(stop)
        return proc

    allocated = set()
    def address():
        n = port()
        while n in allocated:
            n = port()
        allocated.add(n)
        return addr(n)

    def stub(address):
        channel = grpc.insecure_channel(address)
        cleanup.callback(channel.close)
        return rpc.FraudEngineStub(channel)

    url, peer_url = "http://" + address(), "http://" + address()
    launch(["etcd", "--data-dir", directory/"etcd", "--listen-client-urls", url,
            "--advertise-client-urls", url, "--listen-peer-urls", peer_url,
            "--initial-advertise-peer-urls", peer_url, "--initial-cluster", "default="+peer_url])
    until(lambda: urllib.request.urlopen(url+"/health", timeout=1).read())
    members = [[address() for _ in range(3)] for _ in range(shards)]
    mapping = {"partitions":list(range(shards)), "pins":{str(i):i%shards for i in range(8)},
               "members":dict(enumerate(members))}
    def b64(s):
        return base64.b64encode(s.encode()).decode()
    urllib.request.urlopen(url+"/v3/kv/put", json.dumps({"key":b64("/shard_map"),
                           "value":b64(json.dumps(mapping))}).encode(), timeout=3).read()
    procs, clients = {}, {}
    def start(s,i):
        procs[s,i] = launch([BUILD/"fraud_replica", directory/f"s{s}r{i}", str(s), str(i),
                             url, "3", "600000", *members[s]])
    for s in range(shards):
        for i in range(3):
            start(s,i)
            clients[s,i] = stub(members[s][i])
    router_address = address()
    launch([BUILD/"fraud_router",url,router_address])
    router = stub(router_address)
    def healthy():
        response = router.GetClusterHealth(p.HealthRequest(),timeout=5)
        return len(response.shards)==shards and all(
            h.replica_role=="LEADER" and len(h.members)==3 and all(
                m.replica_role in ("LEADER","FOLLOWER") for m in h.members) for h in response.shards)
    until(healthy, 90)
    def update(tenant, seq, mode, timeout=12):
        return router.UpdateVertex(p.UpdateVertexRequest(tenant_id=tenant,vertex_id=seq%64,
                                   volume=seq,mode=mode),timeout=timeout)
    for i in range(8):
        update(i,0,p.QUORUM)
    for name,mode in [("ASYNC",p.ASYNC),("QUORUM",p.QUORUM)]:
        def stream(i):
            rows=[]
            for seq in range(1,33):
                before=clock()
                result=update(i,seq,mode)
                rows.append((shards,name,i,seq,(clock()-before)*1000,result.applied_lsn))
            return rows
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            before=clock()
            rows=list(itertools.chain.from_iterable(pool.map(stream,range(8))))
            duration=clock()-before
        samples.extend(rows)
        lat=[r[-2] for r in rows]
        result=(shards,name,len(lat),duration,len(lat)/duration,percentile(lat,.5),percentile(lat,.99))
        summaries.append(result)
        print("scaling",*result,flush=True)
    if shards != 1:
        return

    events,lag=[],[]
    stop=threading.Event()
    def load():
        seq=1000
        while not stop.is_set():
            before=clock()
            try:
                response=update(0,seq,p.QUORUM,timeout=1)
                outcome="OK"
                epoch=response.epoch
            except grpc.RpcError as failure:
                outcome=failure.code().name
                epoch=0
            events.append((before-origin,clock()-origin,(clock()-before)*1000,outcome,epoch))
            seq+=1
    def monitor():
        while not stop.is_set():
            states=[]
            for i in range(3):
                try:
                    h=clients[0,i].GetClusterHealth(p.HealthRequest(),timeout=.15).shards[0]
                    states.append((h.replica_role,h.applied_lsn,h.epoch))
                except grpc.RpcError:
                    pass
            leaders=[h for h in states if h[0]=="LEADER"]
            followers=[h[1] for h in states if h[0]=="FOLLOWER"]
            delta=max(0,leaders[0][1]-min(followers)) if leaders and followers else ""
            lag.append((clock()-origin,len(states),leaders[0][2] if leaders else "",delta))
            stop.wait(.15)
    old=next(i for i in range(3) if clients[0,i].GetClusterHealth(p.HealthRequest()).shards[0].replica_role=="LEADER")
    origin=clock()
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        jobs=[pool.submit(load),pool.submit(monitor)]
        try:
            time.sleep(2)
            killed=clock()-origin
            procs[0,old].kill()
            procs[0,old].wait()
            until(lambda:any(e[0]>=killed and e[3]=="OK" for e in events),30)
            resumed=next(e[1] for e in events if e[0]>=killed and e[3]=="OK")
            start(0,old)
            until(healthy,30)
            time.sleep(2)
        finally:
            stop.set()
        for job in jobs:
            job.result()
    end=clock()-origin
    write_rows("failover_requests","start_s,end_s,latency_ms,status,epoch",events)
    write_rows("failover_lag","seconds,reachable,leader_epoch,max_follower_lag_records",lag)
    bins=[]
    for i in range(math.ceil(end*2)):
        a=i/2
        b=min(end,a+.5)
        completed=[e for e in events if a<=e[1]<b]
        observations=[x[-1] for x in lag if a<=x[0]<b and x[-1]!=""]
        bins.append((a,b,"before" if b<=killed else "after" if a>=resumed else "failover",
                     sum(e[3]=="OK" for e in completed)/(b-a),sum(e[3]!="OK" for e in completed),
                     percentile([e[2] for e in completed],.99),max(observations,default="")))
    write_rows("failover","start_s,end_s,phase,ack_s,errors,p99_attempt_ms,max_observed_lag",bins)
    write_rows("failover_summary","kill_s,first_postkill_ack_s,recovery_s",[(killed,resumed,resumed-killed)])
    print("failover_seconds",resumed-killed,flush=True)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(["protoc","-I/workspace/proto","--python_out="+tmp,"--grpc_out="+tmp,
                        "--plugin=protoc-gen-grpc="+shutil.which("grpc_python_plugin"),"fraud_engine.proto"],check=True)
        sys.path.insert(0,tmp)
        import fraud_engine_pb2 as p
        import fraud_engine_pb2_grpc as rpc
        summaries,samples=[],[]
        for count in (1,2,4,8):
            with tempfile.TemporaryDirectory() as d, contextlib.ExitStack() as stack:
                try:
                    cluster(count,Path(d),stack,p,rpc,summaries,samples)
                except Exception:
                    print((Path(d)/"service.log").read_text()[-4000:],file=sys.stderr)
                    raise
        write_rows("scaling","shards,mode,writes,seconds,writes_s,p50_ms,p99_ms",summaries)
        write_rows("scaling_requests","shards,mode,client,sequence,latency_ms,applied_lsn",samples)


if __name__=="__main__":
    main()

