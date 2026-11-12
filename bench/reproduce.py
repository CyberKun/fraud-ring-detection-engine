"""Run from any directory with Python 3 and Docker Compose v2."""
import csv
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "bench/results"
os.chdir(ROOT)
OUT.mkdir(exist_ok=True)


def run(*args, capture=False):
    return subprocess.run(args, text=True, check=True, stdout=subprocess.PIPE if capture else None).stdout


revision = run("git","rev-parse","HEAD",capture=True).strip()
run("docker","compose","up","-d","--wait","broker")
network = run("docker","inspect","ringwatch-broker-1","--format","{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}",capture=True).strip()
run("docker","build","-f","Dockerfile.dev","-t","fraud-ring-phase9",".")
image = run("docker","image","inspect","fraud-ring-phase9","--format","{{.Id}}",capture=True).strip()
metadata = {"commit":revision,"command":"python bench/reproduce.py","image":image,"seed":9173,
            "docker":run("docker","info","--format","{{.OperatingSystem}} {{.KernelVersion}} {{.Architecture}} CPUs={{.NCPU}} RAM={{.MemTotal}}",capture=True).strip(),
            "source_changes":run("git","status","--porcelain","--","src","bench",":(exclude)bench/results","Dockerfile.dev","CMakeLists.txt",capture=True).strip()}
(OUT/"run.json").write_text(json.dumps(metadata,indent=2)+"\n")
run("docker","run","--rm","--network",network,"-v",str(OUT)+":/results",
    "-e","FRAUD_TEST_BROKERS=broker:9092","fraud-ring-phase9","bash","-ec", """
set -o pipefail
ctest --test-dir build --output-on-failure | tee /results/ctest.txt
echo topology,engine,manual_per_thousand,sample_index,latency_ns > /tmp/samples.csv
FRAUD_TIMINGS=/tmp/samples.csv ./build/phase9_bench | tee /results/engines.csv
gzip -c /tmp/samples.csv > /results/engine_samples.csv.gz
./build/phase9_bench sizes > /results/sizes.csv
for engine in LCT HDT; do
  gprofng collect app -p hi -o /tmp/$engine.er ./build/phase9_bench $engine
  gprofng display text -functions /tmp/$engine.er > /results/$engine-functions.txt
  gprofng display text -calltree /tmp/$engine.er > /results/$engine-calltree.txt
done
python3 bench/phase9_cluster.py /results
""")
run(os.sys.executable,str(ROOT/"bench/phase9_plots.py"),str(OUT))

