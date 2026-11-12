import base64
import json
import os
import time
import urllib.request

def b64(s):
    return base64.b64encode(s.encode()).decode()

key = b64("/shard_map")
address = os.environ.get("REPLICA_TEMPLATE", "s{s}r{i}:5100")
value = {"partitions": list(range(4))*2, "pins": {},
         "members": {str(s): [address.format(s=s,i=i) for i in range(3)] for s in range(4)}}
txn = {"compare": [{"target":"CREATE","key":key,"create_revision":"0"}],
       "success": [{"request_put":{"key":key,"value":b64(json.dumps(value))}}]}
for attempt in range(60):
    try:
        urllib.request.urlopen("http://etcd:2379/v3/kv/txn", json.dumps(txn).encode(), timeout=2).read()
        break
    except OSError:
        if attempt == 59: raise
        time.sleep(1)
