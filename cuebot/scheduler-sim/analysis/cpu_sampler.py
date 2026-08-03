"""Sample host CPU use over time -> CSV, for plot_run.py.

Every 3s records whole-machine CPU percent plus a per-process-group core count
for the four processes a sim run cares about: cuebot (comm "java"), postgres,
the in-process scheduler thread (comm "cue-scheduler") and the harness's own
python helpers. Group totals come from utime+stime deltas in /proc/<pid>/stat
divided by SC_CLK_TCK, so they read as cores-consumed rather than jiffies. Use
it to tell a scheduler that is genuinely CPU-bound from one that is merely
waiting on the DB -- if postgres owns the cores, the bottleneck is the query
plan, not the planner. Companion to util_sampler.py and db_sampler.py.

usage: cpu_sampler.py <out.csv>
"""
import time, os, sys, glob
OUT=sys.argv[1]; CLK=os.sysconf("SC_CLK_TCK")
def snap():
    """Return (total_jiffies, idle_jiffies, per-group busy jiffies).

    Idle counts both idle and iowait. The per-group dict keys are stable across
    calls so the caller can difference two snapshots directly; processes that
    vanish mid-scan are skipped rather than failing the sample.
    """
    f=open("/proc/stat").readline().split()[1:]
    idle=int(f[3])+int(f[4]); tot=sum(int(x) for x in f)
    g={"cuebot":0,"postgres":0,"scheduler":0,"python":0}
    for p in glob.glob("/proc/[0-9]*/stat"):
        try:
            d=open(p).read(); comm=d.split("(",1)[1].rsplit(")",1)[0]
            r=d.rsplit(")",1)[1].split(); j=int(r[11])+int(r[12])
            if comm=="java": g["cuebot"]+=j
            elif comm.startswith("postgres"): g["postgres"]+=j
            elif comm=="cue-scheduler": g["scheduler"]+=j
            elif comm.startswith("python"): g["python"]+=j
        except: pass
    return tot,idle,g
pt,pi,pg=snap(); pT=time.time()
open(OUT,"w").write("ts,total_cpu_pct,cuebot_cores,postgres_cores,scheduler_cores,python_cores\n")
while True:
    time.sleep(3); t,i,g=snap(); now=time.time(); dt=now-pT; dtot=t-pt
    cpu=100.0*(dtot-(i-pi))/dtot if dtot>0 else 0
    c=lambda k:(g[k]-pg[k])/CLK/dt if dt>0 else 0
    open(OUT,"a").write(f"{time.strftime('%H:%M:%S')},{cpu:.1f},{c('cuebot'):.2f},{c('postgres'):.2f},{c('scheduler'):.2f},{c('python'):.2f}\n")
    pt,pi,pg,pT=t,i,g,now
