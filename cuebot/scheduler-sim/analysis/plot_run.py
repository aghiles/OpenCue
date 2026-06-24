"""Plot a single sim run: utilization% and DB load over time.

Reads <dir>/<tag>_util.csv (util_sampler.py) and <dir>/<tag>_dbstat.csv
(db_sampler.py) and writes <dir>/<tag>_util.png and <dir>/<tag>_dbstats.png.
simulate.py runs this automatically at the end of a watched run; it can also be
run by hand against any sampled run.

usage: plot_run.py <dir> [tag]
"""
import os, sys, csv
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DIR = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SIM_BENCH_DIR", "/tmp/cmp2")
TAG = sys.argv[2] if len(sys.argv) > 2 else "run"


def load(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def elapsed(rows):
    """Seconds since the first sample, from HH:MM:SS timestamps."""
    t0 = None
    xs = []
    for r in rows:
        t = datetime.strptime(r["ts"], "%H:%M:%S")
        if t0 is None:
            t0 = t
        dt = (t - t0).total_seconds()
        if dt < 0:
            dt += 86400          # midnight wrap
        xs.append(dt)
    return xs


made = []

# ---- utilization ----
u = load(os.path.join(DIR, f"{TAG}_util.csv"))
if u:
    x = elapsed(u)
    util = [float(r["util_pct"]) for r in u]
    run = [int(r["running_frames"]) for r in u]
    wait = [int(r["waiting_frames"]) for r in u]
    fig, ax1 = plt.subplots(figsize=(11, 5))
    ax1.plot(x, util, color="tab:blue", lw=2, label="utilization %")
    ax1.set_xlabel("seconds")
    ax1.set_ylabel("utilization %", color="tab:blue")
    ax1.set_ylim(0, 100)
    ax1.grid(True, alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(x, run, color="tab:green", lw=1, alpha=0.7, label="running frames")
    ax2.plot(x, wait, color="tab:orange", lw=1, alpha=0.7, label="waiting frames")
    ax2.set_ylabel("frames")
    lines = ax1.get_lines() + ax2.get_lines()
    ax1.legend(lines, [ln.get_label() for ln in lines], loc="lower right", fontsize=8)
    peak = max(util) if util else 0.0
    ax1.set_title(f"{TAG}: farm utilization (peak {peak:.1f}%)")
    out = os.path.join(DIR, f"{TAG}_util.png")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)
    made.append(out)

# ---- DB load ----
d = load(os.path.join(DIR, f"{TAG}_dbstat.csv"))
if len(d) > 1:
    x = elapsed(d)

    def rate(col):
        vals = [int(r[col]) for r in d]
        out = [0.0]
        for i in range(1, len(vals)):
            dt = x[i] - x[i - 1]
            out.append((vals[i] - vals[i - 1]) / dt if dt > 0 else 0.0)
        return out

    commits = rate("commits")
    ins_r, upd_r, del_r = rate("ins"), rate("upd"), rate("del")
    writes = [ins_r[i] + upd_r[i] + del_r[i] for i in range(len(d))]
    reads = rate("tup_ret")
    lockwait = [int(r["lockwait"]) for r in d]
    fig, ax1 = plt.subplots(figsize=(11, 5))
    ax1.plot(x, commits, color="tab:blue", lw=1.5, label="commits/s")
    ax1.plot(x, writes, color="tab:red", lw=1.5, label="row writes/s (ins+upd+del)")
    ax1.set_xlabel("seconds")
    ax1.set_ylabel("commits / writes per s")
    ax1.grid(True, alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(x, reads, color="tab:gray", lw=1, alpha=0.6, label="tuple reads/s")
    ax2.plot(x, lockwait, color="tab:orange", lw=1.2, label="lock waiters")
    ax2.set_ylabel("reads/s  /  lock waiters")
    lines = ax1.get_lines() + ax2.get_lines()
    ax1.legend(lines, [ln.get_label() for ln in lines], loc="upper right", fontsize=8)
    ax1.set_title(f"{TAG}: DB load")
    out = os.path.join(DIR, f"{TAG}_dbstats.png")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)
    made.append(out)

if made:
    print("wrote:")
    for m in made:
        print(" ", m)
else:
    print("plot_run: no sampler CSVs found in", DIR)
