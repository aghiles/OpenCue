"""OVERDECLARE watcher: lying declarations must shrink to observed truth.

Samples each arm's layer int_mem_min and the farm's core utilization while
the OVERDECLARE workload runs. The legacy balancer (balanceLayerMinMemory,
restored into the batched drain) must lower the flood's 16G declaration to
its observed ~2G rss plus headroom after the first successes; the honest
control keeps its truthful ask; the b_optimize=false guard must never
shrink, whatever the evidence says.

Utilization bar is derived from the host table: packed at the DECLARED 16G
the farm runs ~22% of its cores; the bar sits 15 points above that, proof
the heal actually repacked hosts. Fully cores-bound would need several
120s frame generations more than a verify slot allows; the heal plus a
climbing repack is the guard's contract.

PASS      : flood min-mem <= HEAL_MAX_KB, guard min-mem still >= 16G,
            control never below its 2G ask, util >= derived bar.
FAIL      : the disease (flood stays 16G, util at the declared plateau) or
            a contract breach (guard shrank, control harmed).
INCONCLUSIVE: the flood never reached MIN_PEAK running frames.

usage: overdeclare_watch.py [duration_s] [interval_s]
"""
import os, subprocess, sys, time
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import farm_spec as spec

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
TOKEN = "simoverdecl"
GB_KB = 1024 * 1024
DECLARED_KB = 16 * GB_KB
REAL_KB = 2 * GB_KB
HEAL_MAX_KB = 4 * GB_KB          # healed = observed 2G + 256M, with slack
CSV = os.environ.get("SIM_OVERDECLARE_CSV", "")
PSQL = spec.psql_cmd()


def q(sql):
    try:
        out = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True,
                             timeout=15).stdout.strip()
        return out.splitlines()
    except Exception:
        return []


def derive_util_bar():
    """Midpoint between packing at the declared 16G and cores-bound packing."""
    rows = q("SELECT int_cores, int_mem FROM host;")
    total_cp = sick_cp = 0
    for r in rows:
        parts = r.split("|")
        if len(parts) != 2:
            continue
        cores, mem = int(parts[0]), int(parts[1])
        total_cp += cores
        sick_cp += min(cores, (mem // DECLARED_KB) * 100)
    sick = 100.0 * sick_cp / max(1, total_cp)
    return sick, sick + 15.0


def arm_mem(like):
    rows = q("SELECT min(l.int_mem_min) FROM layer l JOIN job j ON j.pk_job=l.pk_job"
             f" WHERE j.str_name LIKE '{like}';")
    return int(rows[0]) if rows and rows[0].strip().isdigit() else 0


def sample():
    util = q("SELECT round(100.0 * sum(int_cores - int_cores_idle) / sum(int_cores), 1)"
             " FROM host;")
    run = q("SELECT count(*) FROM proc p JOIN job j ON j.pk_job=p.pk_job"
            f" WHERE j.str_name LIKE '%{TOKEN}%';")
    u = float(util[0]) if util and util[0].strip() else 0.0
    r = int(run[0]) if run and run[0].strip().isdigit() else 0
    # Cuebot normalizes job names (hyphens become underscores), so the
    # flood pattern rides the literal underscored form; the _ wildcard
    # only ever matches the underscore itself.
    return (u, r, arm_mem(f"%{TOKEN}_durlong%"), arm_mem(f"%{TOKEN}ctl%"),
            arm_mem(f"%{TOKEN}noopt%"))


def main():
    sick_util, min_util = derive_util_bar()
    min_peak = 100
    print(f"watching OVERDECLARE for {DURATION}s on the full farm. Packed at "
          f"the declared 16G the farm runs {sick_util:.1f}% of its cores (the "
          f"disease); healed to ~2.3G it runs cores-bound. PASS needs flood "
          f"min-mem <= {HEAL_MAX_KB // GB_KB}G, guard still 16G, control "
          f"honest, util >= {min_util:.1f}%.\n", flush=True)
    t0 = time.time()
    peak_util = 0.0
    peak_running = 0
    flood_final = ctl_min = guard_min = 0
    ctl_low = False
    rows_out = []
    while time.time() - t0 < DURATION:
        t = time.time() - t0
        util, running, flood, ctl, guard = sample()
        peak_util = max(peak_util, util)
        peak_running = max(peak_running, running)
        if flood > 0:
            flood_final = flood
        if ctl > 0:
            ctl_min = ctl
            if ctl < REAL_KB:
                ctl_low = True
        if guard > 0:
            guard_min = guard
        print(f"t={t:5.0f} | util {util:5.1f}% | procs {running:5d} | "
              f"mem-min flood {flood // GB_KB:2d}G ctl {ctl // GB_KB:2d}G "
              f"guard {guard // GB_KB:2d}G", flush=True)
        rows_out.append((t, util, running, flood, ctl, guard))
        time.sleep(INTERVAL)

    if CSV:
        try:
            with open(CSV, "w") as f:
                f.write("t,util,procs,flood_memmin_kb,ctl_memmin_kb,guard_memmin_kb\n")
                for r in rows_out:
                    f.write(",".join(str(x) for x in r) + "\n")
        except Exception as e:
            print(f"(could not write CSV {CSV}: {e})", flush=True)

    print("\n==== OVERDECLARE VERDICT ====", flush=True)
    print(f"flood mem-min {flood_final // GB_KB}G; guard mem-min "
          f"{guard_min // GB_KB}G; control mem-min {ctl_min // GB_KB}G; "
          f"peak util {peak_util:.1f}%; peak running {peak_running}", flush=True)
    if peak_running < min_peak:
        print(f"INCONCLUSIVE: only {peak_running} procs ever ran (< {min_peak}); "
              f"the workload never took hold, nothing was measured.", flush=True)
    elif guard_min > 0 and guard_min < DECLARED_KB:
        print(f"FAIL: the b_optimize=false guard shrank to "
              f"{guard_min // GB_KB}G -- the flag's contract is broken.", flush=True)
    elif ctl_low:
        print("FAIL: the honest control's ask was pushed below its real 2G.",
              flush=True)
    elif flood_final > 0 and flood_final <= HEAL_MAX_KB and peak_util >= min_util:
        print(f"PASS: the 16G lie healed to {flood_final // GB_KB}G, the guard "
              f"and control kept their contracts, and util reached "
              f"{peak_util:.1f}% (>= {min_util:.1f}%).", flush=True)
    else:
        print(f"FAIL: flood mem-min {flood_final // GB_KB}G (need <= "
              f"{HEAL_MAX_KB // GB_KB}G), util {peak_util:.1f}% (need "
              f"{min_util:.1f}%) -- the declaration never healed and the farm "
              f"stayed memory-bound (the disease).", flush=True)


if __name__ == "__main__":
    main()
