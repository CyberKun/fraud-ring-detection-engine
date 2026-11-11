"""Exercise four replicated shards and a real Kafka-compatible broker."""
import base64
import concurrent.futures as futures
import contextlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

import grpc
from phase6_chaos import addr, port, until, Proxy


def run(build, root, stack, brokers):
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    proto = Path(__file__).resolve().parents[1] / "proto"
    subprocess.run(["protoc", "-I", str(proto), "--python_out="+str(root), "--grpc_out="+str(root),
                    "--plugin=protoc-gen-grpc="+shutil.which("grpc_python_plugin"), "fraud_engine.proto"], check=True)
    sys.path.insert(0, str(root))
    import fraud_engine_pb2 as p
    import fraud_engine_pb2_grpc as rpc

    # Reserve distinct ports, including across the staggered process starts.
    used = set()
    def fresh():
        n = port()
        while n in used:
            n = port()
        used.add(n)
        return n

    def stop(proc):
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    log = stack.enter_context(open(root/"services.log","w"))
    def launch(args, **opts):
        proc = subprocess.Popen(args, stdout=log, stderr=log, **opts)
        stack.callback(stop, proc)
        return proc
    def client(address):
        channel = grpc.insecure_channel(address, options=[("grpc.initial_reconnect_backoff_ms",100)])
        stack.callback(channel.close)
        return rpc.FraudEngineStub(channel)

    url = "http://"+addr(fresh())
    peer_url = "http://"+addr(fresh())
    launch(["etcd","--data-dir",str(root/"etcd"),"--listen-client-urls",url,"--advertise-client-urls",url,
            "--listen-peer-urls",peer_url,"--initial-advertise-peer-urls",peer_url,"--initial-cluster","default="+peer_url])
    until(lambda: urllib.request.urlopen(url+"/health",timeout=.5).read())
    def etcd(path, body):
        return json.load(urllib.request.urlopen(urllib.request.Request(url+"/v3/kv/"+path,json.dumps(body).encode()),timeout=2))
    def b64(value):
        return base64.b64encode(value.encode()).decode()

    group = "phase7-"+uuid.uuid4().hex
    topic = group
    admin = AdminClient({"bootstrap.servers":brokers})
    admin.create_topics([NewTopic(topic,8,1)],request_timeout=15)[topic].result()
    stack.callback(lambda: admin.delete_topics([topic],request_timeout=10)[topic].result())
    observer = Consumer({"bootstrap.servers":brokers,"group.id":group,"enable.auto.commit":False})
    stack.callback(observer.close)
    def offset(part):
        return observer.committed([TopicPartition(topic,part)],timeout=3)[0].offset

    nodes = {(s,i):fresh() for s in range(4) for i in range(3)}
    metrics = {k:fresh() for k in nodes}
    replicas = {k:client(addr(v)) for k,v in nodes.items()}
    members = {s:[addr(nodes[s,i]) for i in range(3)] for s in range(4)}
    placements = [2,0,3,1,2,0,3,1]
    hot = 101
    def hashed(tenant):
        h = 14695981039346656037
        for b in tenant.to_bytes(8,"little"):
            h = ((h^b)*1099511628211)&((1<<64)-1)
        return h%8
    mapping = {"partitions":placements,"pins":{str(hot):1},"members":members}
    def publish():
        return etcd("put",{"key":b64("/shard_map"),"value":b64(json.dumps(mapping))})
    publish()
    def partition(t):
        return mapping["pins"].get(str(t),hashed(t))
    def shard(t):
        return placements[partition(t)]
    assert placements[hashed(hot)]!=shard(hot)
    tenants = [hot]+list(range(1,9))
    assert len({shard(t) for t in tenants})==4
    links = {(i,j):Proxy(nodes[3,j]) for i in range(3) for j in range(3) if i!=j}
    for proxy in links.values():
        stack.callback(proxy.close)
    procs = {}
    def start(s,i,mode="QUORUM"):
        ports = [links[i,j].address if s==3 and i!=j else members[s][j] for j in range(3)]
        env = dict(os.environ,FRAUD_KAFKA_BROKERS=brokers,FRAUD_KAFKA_TOPIC=topic,FRAUD_KAFKA_GROUP=group,
                   FRAUD_KAFKA_ACK=mode,FRAUD_CHECKPOINT_FRAMES="1" if (s,i)==(0,0) else "16",
                   FRAUD_OFFSET_COMMIT_MS="60000" if (s,i)==(0,0) else "150",
                   FRAUD_REPLICATION_LAG="2" if s==3 else "16384")
        procs[s,i]=launch([build/"fraud_replica",root/f"r{s}-{i}",str(s),str(i),url,"3","600000",
                           *ports,str(metrics[s,i])],env=env)
    def elected(s):
        entries=etcd("range",{"key":b64(f"/shards/{s}/leader")}).get("kvs",[])
        return int(base64.b64decode(entries[0]["value"]).split(b"/")[0]) if entries else -1
    def health(s,i,checksum=False):
        return replicas[s,i].GetClusterHealth(p.HealthRequest(checksum=checksum),timeout=2).shards[0]
    def metric(s,i,name):
        text=urllib.request.urlopen("http://"+addr(metrics[s,i])+"/metrics",timeout=2).read().decode()
        return int(next(line.split()[1] for line in text.splitlines() if line.startswith(name+" ")))
    for s in range(4):
        start(s,0)
    until(lambda: all(elected(s)==0 for s in range(4)))
    for s in range(4):
        for i in [1,2]:
            start(s,i)
    address=addr(fresh())
    launch([build/"fraud_router",url,address])
    router=client(address)
    until(lambda: sum(h.replica_role=="LEADER" for h in router.GetClusterHealth(p.HealthRequest(),timeout=3).shards)==4)
    until(lambda: all(metric(s,0,"kafka_assigned_partitions")==2 for s in range(4)))
    assert all(metric(s,i,"kafka_assigned_partitions")==0 for s in range(4) for i in [1,2])

    def produce(records):
        subprocess.run([build/"fraud_produce",url,brokers,topic],input="".join(json.dumps(x)+"\n" for x in records),
                       text=True,check=True,timeout=30)
    expiry=int(time.time()*1000)+3600000
    def edge(t,n,sequence=1):
        return {"upsert":{"tenant_id":t,"u":0,"v":n,"expiry_ms":expiry,
                          "source":{"producer_id":n,"sequence":sequence}}}
    def graph(t,minimum=0):
        return router.GetComponentSubgraph(p.SubgraphRequest(tenant_id=t,vertex_id=0,max_nodes=200,min_lsn=minimum),timeout=4)
    def weight(t,v,value,source,seq=1,mode=p.QUORUM):
        return router.UpdateVertex(p.UpdateVertexRequest(tenant_id=t,vertex_id=v,volume=value,mode=mode,
                                   source=p.ProducerSequence(producer_id=source,sequence=seq)),timeout=8)

    # No earlier messages exist on this pinned partition. Wait for durable graph state,
    # verify the broker position, then kill before the first offset flush.
    produce([edge(hot,70)])
    until(lambda: len(graph(hot).edges)==1)
    assert offset(1)<1 and health(0,0).commit_lsn==1
    assert (root/"r0-0"/"replica.snapshot").is_file()
    stop(procs[0,0])
    resumed=weight(hot,70,700,1000)
    assert resumed.applied_lsn==2
    leader=elected(0)
    assert leader in (1,2)
    until(lambda: offset(1)==1)
    assert metric(0,leader,"kafka_duplicate_records_total")>=1
    start(0,0)
    until(lambda: health(0,0).applied_lsn>=2)
    assert len(graph(hot).edges)==1 and graph(hot).edges[0].version==1
    extension=edge(hot,70,2)
    extension["upsert"]["expiry_ms"]=expiry+1000
    conflicting=edge(hot,70,2)
    conflicting["upsert"]["expiry_ms"]=expiry+2000
    duplicate_weight={"update":{"tenant_id":hot,"vertex_id":70,"volume":99999,
                                "source":{"producer_id":1000,"sequence":1}}}
    produce([extension,conflicting,duplicate_weight])
    until(lambda: offset(1)==4)
    hot_graph=graph(hot)
    assert hot_graph.edges[0].version==2 and hot_graph.edges[0].expiry_ms==expiry+1000
    assert next(v.volume for v in hot_graph.vertices if v.vertex_id==70)==700
    print("crash_after_quorum_before_offset=passed transparent_router_retry=passed redelivery_transitions=0",flush=True)

    # Fill the majority replication lag while both followers remain healthy for reads.
    lag_t=next(t for t in tenants if shard(t)==3)
    for proxy in links.values():
        proxy.blocked.set()
    time.sleep(.2)
    produce([edge(lag_t,72)])
    until(lambda: health(3,0).last_lsn>health(3,0).commit_lsn)
    time.sleep(.3)  # more than one configured offset flush interval
    assert offset(partition(lag_t))<1
    for proxy in links.values():
        proxy.blocked.clear()
    until(lambda: offset(partition(lag_t))==1)
    for proxy in links.values():
        proxy.blocked.set()
    time.sleep(.2)
    a=weight(lag_t,0,123,2000,mode=p.ASYNC)
    b=weight(lag_t,0,124,2000,seq=2,mode=p.ASYNC)
    r=router.ComponentStats(p.ComponentStatsRequest(tenant_id=lag_t,vertex_id=0,min_lsn=b.applied_lsn))
    assert r.replica_role=="LEADER" and r.volume==124
    try:
        weight(lag_t,0,125,2000,seq=3)
        raise AssertionError("lagging leader admitted write")
    except grpc.RpcError as err:
        assert err.code()==grpc.StatusCode.RESOURCE_EXHAUSTED, err
    produce([edge(lag_t,71)])
    until(lambda: metric(3,0,"kafka_pause_total")>0)
    assert offset(partition(lag_t))==1
    for proxy in links.values():
        proxy.blocked.clear()
    until(lambda: metric(3,0,"kafka_resume_total")>0 and offset(partition(lag_t))==2)
    assert metric(3,0,"shed_requests_total")>=2
    print("lag_shedding=passed kafka_pause_resume=passed min_lsn_leader_fallback=passed",flush=True)

    # Update an unused pin at a higher etcd revision and exercise both routes.
    extra=200
    mapping["pins"][str(extra)]=6
    publish()
    tenants.append(extra)
    time.sleep(.25)
    batch=[]
    def width(t):
        return 64 if t==hot else 16
    for t in tenants:
        for v in range(1,width(t)+1):
            batch.extend([edge(t,v),edge(t,v)])
    def grpc_stream(t):
        samples=[]
        for v in range(1,width(t)+1):
            start=time.perf_counter()
            weight(t,v,t*100+v,3000+v)
            samples.append((time.perf_counter()-start)*1000)
        return samples
    start_time=time.perf_counter()
    with futures.ThreadPoolExecutor(len(tenants)+1) as pool:
        kafka=pool.submit(produce,batch)
        results=[pool.submit(grpc_stream,t) for t in tenants]
        latencies=[x for result in results for x in result.result()]
        kafka.result()
    # Broker high water includes duplicate deliveries. Wait until every assigned
    # offset is committed, then measure end-to-end throughput.
    for part in range(8):
        high=observer.get_watermark_offsets(TopicPartition(topic,part),timeout=3)[1]
        until(lambda: offset(part)==high)
    duration=time.perf_counter()-start_time
    n=sum(width(t) for t in tenants)
    for t in tenants:
        expected=set(range(1,width(t)+1))
        if t==hot:expected.add(70)
        if t==lag_t:expected.update([71,72])
        g=graph(t)
        assert {e.v for e in g.edges}==expected and all(e.u==0 and e.version==(2 if e.v==70 else 1) and e.expiry_ms==expiry+(1000 if e.v==70 else 0) for e in g.edges)
        weights={v.vertex_id:v.volume for v in g.vertices}
        assert weights=={0:124 if t==lag_t else 0,**{v:t*100+v for v in range(1,width(t)+1)},
                         **({70:700} if t==hot else {}),**({71:0,72:0} if t==lag_t else {})}
        assert g.replica_role=="FOLLOWER"
        r=router.AuditAt(p.AuditAtRequest(tenant_id=t,vertex_id=0,timestamp_ms=expiry,max_nodes=200,min_lsn=g.applied_lsn))
        assert r.applied_lsn>=g.applied_lsn and r.stats.volume==sum(weights.values())
        for s in range(4):
            if s!=shard(t):
                assert not replicas[s,elected(s)].Connected(p.ConnectedRequest(tenant_id=t,u=0,v=1)).connected
    for s in range(4):
        i=elected(s)
        expected=subprocess.check_output([build/"phase7_oracle",root/f"r{s}-{i}"/"replica.wal"],text=True).strip()
        until(lambda: all(health(s,j,True).checksum==expected for j in range(3)))
    states=sum(health(s,elected(s)).applied_lsn for s in range(4))
    assert states==2*n+7, states
    rank=math.ceil(.99*n)-1
    print(f"shards=4 replicas=12 tenants={len(tenants)} tenant_isolation=100% oracle_checksums=12/12 "
          f"unique_transitions={states} duplicate_kafka_deliveries={n} hot_to_normal_load=4:1",flush=True)
    print(f"grpc_quorum_writes={n} kafka_unique_writes={n} kafka_deliveries={2*n} seconds={duration:.3f} "
          f"unique_writes_per_second={2*n/duration:.2f} grpc_ack_p99_ms={sorted(latencies)[rank]:.2f}",flush=True)
    for s in range(4):
        t=next(t for t in tenants if shard(t)==s)
        read=p.ConnectedRequest(tenant_id=t,u=0,v=1)
        seen=set()
        for _ in range(6):
            response,call=router.Connected.with_call(read,timeout=2)
            assert response.replica_role=="FOLLOWER" and response.connected
            seen.add(dict(call.trailing_metadata())["replica-address"])
        assert len(seen)==2
        alerts=router.SubscribeAlerts(p.SubscribeAlertsRequest(tenant_id=t),timeout=8)
        alerts.initial_metadata()
        size=graph(t).total_vertices
        for v in range(1,math.ceil(size*.25)+1):
            router.UpdateVertex(p.UpdateVertexRequest(tenant_id=t,vertex_id=v,is_flagged=True,volume=t*100+v),timeout=3)
        event=next(alerts)
        assert event.tenant_id==t and event.stats.flagged_ratio>=.25
        alerts.cancel()
        removed=router.DeleteEdge(p.DeleteEdgeRequest(tenant_id=t,u=0,v=1),timeout=3)
        assert not router.Connected(p.ConnectedRequest(tenant_id=t,u=0,v=1,min_lsn=removed.applied_lsn)).connected
    print("follower_read_distribution=2/2_per_shard tenant_alert_streams=4/4 routed_deletes=4/4",flush=True)
    # Repeat the isolated Kafka append with explicitly configured ASYNC consumers.
    for i in range(3):
        stop(procs[3,i])
    until(lambda: elected(3)==-1)
    start(3,0,"ASYNC")
    until(lambda: elected(3)==0)
    for i in (1,2):
        start(3,i,"ASYNC")
    until(lambda: metric(3,0,"kafka_assigned_partitions")==2)
    old=until(lambda: health(3,0)).commit_lsn
    pos=offset(partition(lag_t))
    for proxy in links.values():
        proxy.blocked.set()
    time.sleep(.2)
    produce([edge(lag_t,73)])
    until(lambda: offset(partition(lag_t))==pos+1)
    isolated=health(3,0)
    assert isolated.applied_lsn==isolated.last_lsn==old+1 and isolated.commit_lsn==old
    for proxy in links.values():
        proxy.blocked.clear()
    until(lambda: all(health(3,i).commit_lsn==old+1 for i in range(3)))
    print("kafka_async_offset_after_local_fsync=passed kafka_quorum_offset_after_replication=passed",flush=True)


if __name__=="__main__":
    brokers=os.environ.get("FRAUD_TEST_BROKERS")
    if not brokers:
        print("Set FRAUD_TEST_BROKERS to run the four-shard real-broker gate")
        sys.exit(77)
    with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as cleanup:
        try:
            run(Path(sys.argv[1]).resolve(),Path(tmp),cleanup,brokers)
        except Exception:
            path=Path(tmp)/"services.log"
            if path.exists():print(path.read_text()[-4000:],file=sys.stderr)
            raise
