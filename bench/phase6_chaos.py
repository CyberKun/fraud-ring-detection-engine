"""Run against a temporary real etcd and three independently restartable processes."""
import base64
import concurrent.futures
import contextlib
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import socket
import socketserver
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

import grpc


def until(fn, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (grpc.RpcError, OSError):
            pass
        time.sleep(.03)
    raise AssertionError(f"timeout waiting for {fn}")


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def addr(p):
    return f"127.0.0.1:{p}"


class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        if self.server.blocked.is_set():
            return
        try:
            with socket.create_connection(self.server.target, .5) as dst:
                dst.settimeout(None)
                for sock in (self.request, dst):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                endpoints = (self.request, dst)
                while not self.server.blocked.is_set():
                    for sock in select.select(endpoints, [], [], .02)[0]:
                        data = sock.recv(65536)
                        if not data:
                            return
                        (dst if sock is self.request else self.request).sendall(data)
        except OSError:
            pass


class Proxy(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, target):
        super().__init__(("127.0.0.1", 0), Forward)
        self.target = ("127.0.0.1", target) if isinstance(target, int) else target
        self.blocked = threading.Event()
        self.address = addr(self.server_address[1])
        threading.Thread(target=self.serve_forever, args=(.05,), daemon=True).start()

    def close(self):
        self.blocked.set()
        self.shutdown()
        self.server_close()


def run(binary, directory, cleanup):
    proto = Path(__file__).resolve().parents[1] / "proto"
    subprocess.run(["protoc", "-I", str(proto), "--python_out="+directory,
                    "--grpc_out="+directory, "--plugin=protoc-gen-grpc="+shutil.which("grpc_python_plugin"),
                    "fraud_engine.proto"], check=True)
    sys.path.insert(0, directory)
    import fraud_engine_pb2 as p
    import fraud_engine_pb2_grpc as rpc

    root = Path(directory)
    def kill(proc):
        if proc and proc.poll() is None:
            proc.kill()
            proc.wait()

    external_host=os.environ.get("PHASE6_ETCD_HOST")
    client=(external_host,2379) if external_host else ("127.0.0.1",port())
    url="http://%s:%s"%client
    if not external_host:
        peer_url = "http://"+addr(port())
        etcd = subprocess.Popen(["etcd", "--data-dir", str(root/"etcd"), "--listen-client-urls", url,
                                 "--advertise-client-urls", url, "--listen-peer-urls", peer_url,
                                 "--initial-advertise-peer-urls", peer_url, "--initial-cluster", "default="+peer_url],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cleanup.callback(kill, etcd)
    until(lambda: urllib.request.urlopen(url+"/health", timeout=.3).read())
    key = base64.b64encode(b"/shards/0/leader").decode()

    def election():
        request = urllib.request.Request(url+"/v3/kv/range", json.dumps({"key":key}).encode())
        items = json.load(urllib.request.urlopen(request, timeout=.5)).get("kvs", [])
        return base64.b64decode(items[0]["value"]).decode() if items else ""

    ports = [port() for _ in range(3)]
    links = {(i,j):Proxy(ports[j]) for i in range(3) for j in range(3) if i != j}
    leases = [Proxy(client) for _ in ports]
    for proxy in [*links.values(), *leases]:
        cleanup.callback(proxy.close)
    channels = [grpc.insecure_channel(addr(n), options=[("grpc.initial_reconnect_backoff_ms", 100)]) for n in ports]
    api = [rpc.FraudEngineStub(c) for c in channels]
    internal = [rpc.ReplicaPeerStub(c) for c in channels]
    procs = [None]*3

    def start(i, tick_ms=600000):
        endpoints = [addr(ports[i]) if j == i else links[i,j].address for j in range(3)]
        procs[i] = subprocess.Popen([binary, str(root/str(i)), "0", str(i), "http://"+leases[i].address,
                                     "2", str(tick_ms), *endpoints])
        cleanup.callback(kill, procs[i])

    def health(i, checksum=False):
        return api[i].GetClusterHealth(p.HealthRequest(checksum=checksum), timeout=1).shards[0]

    def partition(i, blocked, include_lease=True):
        for pair, proxy in links.items():
            if i in pair:
                (proxy.blocked.set if blocked else proxy.blocked.clear)()
        if include_lease:
            (leases[i].blocked.set if blocked else leases[i].blocked.clear)()

    def denied(fn):
        try:
            fn()
        except grpc.RpcError as error:
            assert error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.FAILED_PRECONDITION), error
        else:
            raise AssertionError("operation unexpectedly acknowledged")

    def fenced(i, stale):
        try:
            internal[i].Exchange(stale,timeout=1)
        except grpc.RpcError as error:
            assert error.code()==grpc.StatusCode.FAILED_PRECONDITION and "stale epoch" in error.details(), error
        else:
            raise AssertionError("stale-epoch append accepted")

    tenant = 7
    expiry = int(time.time()*1000)+3600000
    def edge(i, n, mode=p.QUORUM):
        return api[i].UpsertEdge(p.UpsertEdgeRequest(tenant_id=tenant, u=0, v=n, expiry_ms=expiry, mode=mode,
                                source=p.ProducerSequence(producer_id=n, sequence=1)), timeout=4)
    def confirmed_edge(i,n):
        # A timeout can follow a durable append. Retry the identical producer
        # sequence and count it only once a client receives success.
        def attempt():
            try:
                return edge(i,n)
            except grpc.RpcError as error:
                if error.code() not in (grpc.StatusCode.UNAVAILABLE,grpc.StatusCode.DEADLINE_EXCEEDED):
                    raise AssertionError(error) from error
                return False
        return until(attempt)

    def connected(i, v, barrier=0):
        return api[i].Connected(p.ConnectedRequest(tenant_id=tenant,u=0,v=v,min_lsn=barrier),timeout=1)
    def converge():
        hs = [health(i, True) for i in range(3)]
        return hs if len({(h.applied_lsn,h.commit_lsn,h.last_lsn,h.checksum,h.log_hash) for h in hs})==1 and hs[0].commit_lsn==hs[0].last_lsn else None

    start(0)
    until(lambda: election().startswith("0/"))
    start(1)
    until(lambda: health(0).replica_role=="LEADER")
    partition(2, True, include_lease=False)
    start(2)
    acknowledged = set()
    for n in range(1,25):
        confirmed_edge(0,n)
        acknowledged.add(n)
    # Multiple independent producer streams remain outstanding at the instant of SIGKILL.
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        outstanding = {pool.submit(edge,0,n):n for n in range(25,49)}
        until(lambda: any(f.done() and f.exception() is None for f in outstanding))
        assert any(not f.done() for f in outstanding)
        begin=time.monotonic()
        kill(procs[0])
        kill(procs[1])  # force the behind replica to win the next lease
        partition(2,False)
        for f,n in outstanding.items():
            try:
                f.result()
                acknowledged.add(n)
            except grpc.RpcError:
                pass
    until(lambda: election().startswith("2/"))
    elected=time.monotonic()
    assert health(2).last_lsn==0
    denied(lambda: edge(2,10000))
    start(1)
    until(lambda: health(2).replica_role=="LEADER")
    first=confirmed_edge(2,49)
    resumed=time.monotonic()
    assert health(2).fetched_frames>=len(acknowledged)
    assert all(connected(2,n,first.applied_lsn).connected for n in acknowledged)
    print(f"A prekill_acknowledged={len(acknowledged)} lost=0 election_ms={(elected-begin)*1000:.1f} "
          f"first_quorum_ack_ms={(resumed-begin)*1000:.1f} fetched_frames={health(2).fetched_frames}",flush=True)
    start(0)
    until(converge)
    lags=[]
    for n in range(1,81):
        a=confirmed_edge(2,n)
        lags.extend(max(0,a.applied_lsn-health(i).applied_lsn) for i in [0,1])
    baseline=until(converge)[0].commit_lsn
    for i in range(3):
        graph=api[i].GetComponentSubgraph(p.SubgraphRequest(tenant_id=tenant,vertex_id=0,max_nodes=100,min_lsn=baseline))
        assert {(e.u,e.v) for e in graph.edges}=={(0,n) for n in range(1,81)}
        assert {v.vertex_id for v in graph.vertices}==set(range(81))
        assert graph.replica_role==("LEADER" if i==2 else "FOLLOWER")
        denied(lambda: connected(i,1,baseline+100))
        denied(lambda: api[i].ComponentStats(p.ComponentStatsRequest(tenant_id=tenant,vertex_id=0,min_lsn=baseline+100),timeout=1))
        denied(lambda: api[i].GetComponentSubgraph(p.SubgraphRequest(tenant_id=tenant,vertex_id=0,min_lsn=baseline+100),timeout=1))
    print(f"A follower_lag_lsn samples={len(lags)} median={statistics.median(lags):g} "
          f"p99={sorted(lags)[int(.99*(len(lags)-1))]} max={max(lags)} converged_lsn={baseline}",flush=True)

    owner=election()
    epoch=health(2).epoch
    # Keep etcd reachable initially so local-only ASYNC can succeed during peer isolation.
    partition(2,True,include_lease=False)
    time.sleep(.15)
    isolated=edge(2,999,p.ASYNC)
    assert isolated.applied_lsn>baseline and health(2).commit_lsn==baseline
    denied(lambda: edge(2,998))
    assert health(2).last_lsn>isolated.applied_lsn
    for i in [0,1]:
        assert not connected(i,999).connected
    leases[2].blocked.set()
    begin=time.monotonic()
    def promoted():
        for i in [0,1]:
            if health(i).replica_role=="LEADER":
                return (i,)
    until(lambda: election().startswith(("0/","1/")))
    elected=time.monotonic()
    leader=until(promoted)[0]
    new=confirmed_edge(leader,81)
    resumed=time.monotonic()
    assert new.epoch>epoch
    denied(lambda: edge(2,997,p.ASYNC))
    stale=p.PeerRequest(shard=0,owner=owner,epoch=epoch,op=p.APPEND)
    for i in [0,1]:
        fenced(i,stale)
    partition(2,False)
    hs=until(converge)
    assert hs[2].truncations>=1 and hs[2].last_entry_epoch==new.epoch
    for i in range(3):
        assert not connected(i,999).connected and not connected(i,998).connected
    # Exercise all mutation types and compare the resulting graph/weights with an independent oracle.
    api[leader].UpdateVertex(p.UpdateVertexRequest(tenant_id=tenant,vertex_id=0,volume=123,is_flagged=True))
    end=api[leader].DeleteEdge(p.DeleteEdgeRequest(tenant_id=tenant,u=0,v=81)).applied_lsn
    hs=until(converge)
    for i in range(3):
        req=p.ComponentStatsRequest(tenant_id=tenant,vertex_id=0,min_lsn=end)
        stats=api[i].ComponentStats(req)
        assert (stats.vertex_count,stats.account_count,stats.volume,stats.flagged_accounts)==(81,81,123,1)
        assert stats.replica_role==("LEADER" if i==leader else "FOLLOWER")
        assert connected(i,1,end).replica_role==stats.replica_role
        graph=api[i].GetComponentSubgraph(p.SubgraphRequest(tenant_id=tenant,vertex_id=0,max_nodes=100,min_lsn=end))
        assert {(e.u,e.v) for e in graph.edges}=={(0,n) for n in range(1,81)}
        assert not connected(i,81,end).connected
    hashes={hashlib.sha256((root/str(i)/"replica.wal").read_bytes()).hexdigest() for i in range(3)}
    assert len(hashes)==1
    kill(procs[2]);start(2)
    until(lambda: health(2).commit_lsn==end)
    fenced(2,stale)
    assert health(2,True).checksum==hs[leader].checksum
    print(f"B partition_election_ms={(elected-begin)*1000:.1f} first_quorum_ack_ms={(resumed-begin)*1000:.1f} "
          f"truncations={hs[2].truncations} end_lsn={end} logical_sha256={hs[0].checksum} wal_sha256={hashes.pop()}",flush=True)

    # Give the isolated follower a shorter timer than the edge TTL. Only
    # received leader frames may expire the edge or move its applied watermark.
    for proc in procs:
        kill(proc)
    for i in range(3):
        start(i, tick_ms=120)
    def active():
        for i in range(3):
            if health(i).replica_role=="LEADER":
                return (i,)
    leader=until(active)[0]
    follower=(leader+1)%3
    a=api[leader].UpsertEdge(p.UpsertEdgeRequest(tenant_id=tenant,u=0,v=82,
                                expiry_ms=int(time.time()*1000)+1800), timeout=4)
    until(lambda: connected(follower,82,a.applied_lsn).connected)
    partition(follower,True,include_lease=False)
    time.sleep(.15)
    frozen=health(follower).applied_lsn
    until(lambda: not connected(leader,82).connected)
    assert connected(follower,82).connected and health(follower).applied_lsn==frozen
    partition(follower,False)
    until(lambda: not connected(follower,82).connected)
    print("Idle expiry: isolated follower watermark held; leader Tick/Expire replayed on heal",flush=True)


if __name__=="__main__":
    with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as cleanup:
        run(os.path.abspath(sys.argv[1]),directory,cleanup)
