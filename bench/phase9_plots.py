"""Plot measured CSV columns; blank observations remain gaps in the trace."""
import csv
import math
import sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

out=Path(sys.argv[1])
def rows(name):
    with (out/(name+".csv")).open() as f:
        return list(csv.DictReader(f))

def comparison(data,group,x,ylabels,file,xlabel):
    fig,axes=plt.subplots(1,len(ylabels),figsize=(5*len(ylabels),3.6),squeeze=False)
    for ax,(key,title) in zip(axes[0],ylabels):
        for name in dict.fromkeys(r[group] for r in data):
            series=[r for r in data if r[group]==name]
            ax.plot([float(r[x]) for r in series],[float(r[key]) for r in series],"o-",label=name)
        ax.set(xlabel=xlabel,ylabel=title)
        ax.grid(alpha=.25)
        ax.legend()
    fig.tight_layout()
    fig.savefig(out/(file+".svg"))
    plt.close(fig)

comparison([r for r in rows("engines") if r["topology"]=="sweep"],"engine","manual_pct",
           [("ops_s","Operations / second"),("p99_us","p99 (µs)"),("p999_us","p99.9 (µs)")],"crossover","Manual tree deletes (% of mutations)")
comparison(rows("scaling"),"mode","shards",[("writes_s","Acknowledged writes / second"),("p99_ms","p99 (ms)")],
           "scaling","Shards")
data=rows("failover")
summary=rows("failover_summary")[0]
fig,axes=plt.subplots(3,1,figsize=(9,6),sharex=True)
for ax,(key,label) in zip(axes,[("ack_s","ACK / s"),("p99_attempt_ms","p99 attempt (ms)"),("max_observed_lag","LSN lag")]):
    ax.step([float(r["start_s"]) for r in data],[float(r[key]) if r[key] else math.nan for r in data],where="post")
    ax.axvspan(float(summary["kill_s"]),float(summary["first_postkill_ack_s"]),color="orange",alpha=.22)
    ax.set_ylabel(label)
axes[-1].set_xlabel("Seconds from load start (shading ends at first successful post-kill request)")
fig.tight_layout()
fig.savefig(out/"failover.svg")

