"""2x2 grid: {new,rust} x {compress2,compress8}. Steady-state medians (t90-180)."""
import os
import statistics as st
CMP = os.environ.get("SIM_BENCH_DIR", "/tmp/cmp2")
# logical tag -> file prefix
CELLS=[("new  @2","before"),("new  @8","new8"),("rust @2","rust2"),("rust @8","rust8")]
def sod(h):
    """Convert an "HH:MM:SS" stamp to seconds-since-midnight."""
    a=list(map(int,h.split(":"))); return a[0]*3600+a[1]*60+a[2]
def sim_t0(pre):
    """Return the wall-clock second the run's measurement window opened.

    Anchors on the first "util=" line the sim logged, which is emitted once the
    farm is registered and the watch loop starts -- everything before that is
    setup and must not count against the steady-state window. Returns None when
    the log is missing or never reached that point, which the callers treat as
    "this cell didn't run" and render as "-".
    """
    try:
        for ln in open(f"{CMP}/{pre}_sim.log",errors="ignore"):
            if "] util=" in ln:
                return sod(ln[1:9])
    except FileNotFoundError: return None
    return None
def med_sim(pre,key,lo,hi):
    """Median of a "<key>=<number>" field scraped from a run's sim log.

    Only samples whose offset from sim_t0 falls in [lo, hi] count, so the
    result is a steady-state median rather than an average dragged down by the
    farm filling up. Returns None if the log or the key is absent.
    """
    import re
    t0=sim_t0(pre)
    if t0 is None: return None
    rx=re.compile(r"\[(\d\d:\d\d:\d\d)\].*?"+key+r"=\s*([\d.]+)")
    v=[]
    for ln in open(f"{CMP}/{pre}_sim.log",errors="ignore"):
        m=rx.search(ln)
        if m:
            t=sod(m.group(1))-t0
            if lo<=t<=hi: v.append(float(m.group(2)))
    return st.median(v) if v else None
def med_csv(pre,suffix,colidx,lo,hi,rate=False):
    """Median of one column of a run's sampler CSV over the [lo, hi] window.

    With rate=False the column is read as a gauge and its values are taken
    directly. With rate=True it is read as a cumulative counter and
    consecutive rows are differenced into a per-second rate -- which is what
    the pg_stat_database columns need, since postgres only ever reports
    running totals. Returns None when the file is missing or nothing lands in
    the window.
    """
    t0=sim_t0(pre)
    if t0 is None: return None
    try: rows=[ln.strip().split(",") for ln in open(f"{CMP}/{pre}_{suffix}.csv",errors="ignore")][1:]
    except FileNotFoundError: return None
    data=[]
    for r in rows:
        try: data.append((sod(r[0]),[float(x) for x in r[1:]]))
        except: pass
    if not data: return None
    v=[]
    if rate:
        for i in range(1,len(data)):
            (ta,a),(tb,b)=data[i-1],data[i]; dt=tb-ta
            if dt<=0: continue
            t=tb-t0
            if lo<=t<=hi: v.append((b[colidx]-a[colidx])/dt)
    else:
        for ts,c in data:
            t=ts-t0
            if lo<=t<=hi and colidx < len(c): v.append(c[colidx])
    return st.median(v) if v else None
# dbstat cols (after ts): 0 commits,1 rollbacks,2 tup_ret,3 tup_fetch,4 ins,5 upd,6 del,7 deadlk,8 blks_read,9 blks_hit,10 active,11 lockwait
# cpu cols (after ts): 0 total_cpu%,1 cuebot,2 postgres,3 scheduler,4 python
LO,HI=90,180
print(f"{'cell':8} {'util%':>6} {'done/s':>7} {'orphan':>7} {'reads/s':>9} {'writes/s':>9} {'rollbk/s':>8} {'lockwt':>6} {'CPU%':>5} {'pg':>5} {'cuebot':>6} {'rust':>5} {'py':>5}")
for name,pre in CELLS:
    util=med_sim(pre,"util",LO,HI); done=med_sim(pre,"done/s",LO,HI); orp=med_sim(pre,"orphan",LO,HI)
    def rd():
        """Combined read rate for this cell: tuples returned + tuples fetched."""
        a=med_csv(pre,"dbstat",2,LO,HI,True); b=med_csv(pre,"dbstat",3,LO,HI,True)
        return (a+b) if (a is not None and b is not None) else None
    def wr():
        """Combined write rate for this cell: inserts + updates + deletes."""
        xs=[med_csv(pre,"dbstat",i,LO,HI,True) for i in (4,5,6)]
        return sum(x for x in xs if x is not None) if any(x is not None for x in xs) else None
    reads=rd(); writes=wr(); rb=med_csv(pre,"dbstat",1,LO,HI,True); lw=med_csv(pre,"dbstat",11,LO,HI,False)
    cpu=med_csv(pre,"cpu",0,LO,HI,False); pg=med_csv(pre,"cpu",2,LO,HI,False)
    cb=med_csv(pre,"cpu",1,LO,HI,False); ru=med_csv(pre,"cpu",3,LO,HI,False); py=med_csv(pre,"cpu",4,LO,HI,False)
    def f(x,d=0):
        """Format a metric to d decimals, rendering a missing value as "-"."""
        return ("%.{}f".format(d)%x) if x is not None else "-"
    print(f"{name:8} {f(util):>6} {f(done):>7} {f(orp):>7} {f(reads):>9} {f(writes):>9} {f(rb,1):>8} {f(lw,2):>6} {f(cpu):>5} {f(pg,2):>5} {f(cb,2):>6} {f(ru,2):>5} {f(py,2):>5}")
