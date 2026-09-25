"""PRIOLAYERS watcher: the higher-priority job holds the contested cores.

Samples the cores each job holds (HI: priority 80, one layer; LO: priority
30, many layers). The invariant, from the legacy dispatcher's job walk:
while both jobs have work, HI takes the cores first and holds most of them.

The verdict reads the last WINDOW_S seconds of samples, past the first fill
and one replacement wave, and needs contention (both jobs still waiting) and
a full farm, or it is INCONCLUSIVE.

PASS      : HI's mean share of the two jobs' cores is at least MIN_HI_SHARE.
FAIL      : the disease. LO holds most of the cores: the draw is per layer
            and each layer is capped to a quarter of every host, so LO's
            layer count beats HI's priority.
INCONCLUSIVE: the farm never filled, or a job ran out of waiting frames.

usage: priolayers_watch.py [duration_s] [interval_s]
"""
import os, subprocess, sys, time
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import farm_spec as spec

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 180
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
CLASSES = ("hijob", "lojob")
TOKEN = "simpriolayers"
WINDOW_S = 30.0
MIN_HI_SHARE = float(os.environ.get("SIM_PRIOLAYERS_MIN_HI_SHARE", "0.6"))
MIN_UTIL = 85.0
PSQL = spec.psql_cmd()


def q(sql):
    try:
        return subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True,
                              timeout=15).stdout.strip().splitlines()
    except Exception:
        return []


def scalar(sql):
    rows = q(sql)
    return int(rows[0]) if rows and rows[0].strip().lstrip("-").isdigit() else 0


def sample():
    util = q("SELECT round(100.0 * sum(int_cores - int_cores_idle) / sum(int_cores), 1)"
             " FROM host;")
    u = float(util[0]) if util and util[0].strip() else 0.0
    cores, wait, pri = {}, {}, {}
    for c in CLASSES:
        sel = f"j.str_name LIKE '%{TOKEN}%' AND j.str_name LIKE '%{c}%'"
        cores[c] = scalar("SELECT COALESCE(sum(p.int_cores_reserved),0) FROM proc p"
                          f" JOIN job j ON j.pk_job=p.pk_job WHERE {sel};")
        wait[c] = scalar("SELECT count(*) FROM frame f JOIN job j ON j.pk_job=f.pk_job"
                         f" WHERE {sel} AND f.str_state='WAITING';")
        pri[c] = scalar("SELECT COALESCE(max(jr.int_priority),0) FROM job_resource jr"
                        f" JOIN job j ON j.pk_job=jr.pk_job WHERE {sel};")
    return u, cores, wait, pri


def main():
    print(f"watching PRIOLAYERS for {DURATION}s. PASS needs the higher-priority job to "
          f"hold at least {MIN_HI_SHARE:.0%} of the two jobs' cores over the last "
          f"{WINDOW_S:.0f}s, both jobs still waiting.\n", flush=True)
    t0 = time.time()
    rows = []
    peak_util = 0.0
    pri = {}
    while time.time() - t0 < DURATION:
        t = time.time() - t0
        util, cores, wait, cur = sample()
        if cur != pri and all(cur.values()):
            pri = cur
            print("priorities: " + ", ".join(f"{c} {pri[c]}" for c in CLASSES), flush=True)
        peak_util = max(peak_util, util)
        both = sum(cores.values())
        share = cores["hijob"] / both if both else 0.0
        print(f"t={t:5.0f} | util {util:5.1f}% | "
              + " | ".join(f"{c} {cores[c] // 100:5d} cores wait {wait[c]:5d}"
                           for c in CLASSES)
              + f" | hi share {share:4.2f}", flush=True)
        rows.append((t, util, share, wait))
        time.sleep(INTERVAL)

    if not rows:
        print("\n==== PRIOLAYERS VERDICT ====\nINCONCLUSIVE: nothing sampled.", flush=True)
        return
    tail = [r for r in rows if r[0] >= DURATION - WINDOW_S] or rows[-3:]
    share = sum(r[2] for r in tail) / len(tail)
    contended = all(r[3][c] > 0 for r in tail for c in CLASSES)
    ratio = (1 - share) / share if share > 0 else float("inf")
    print("\n==== PRIOLAYERS VERDICT ====", flush=True)
    print(f"peak util {peak_util:.1f}%; last {WINDOW_S:.0f}s mean hi share {share:.2f}; "
          f"lo/hi cores {ratio:.1f}x", flush=True)
    if peak_util < MIN_UTIL:
        print(f"INCONCLUSIVE: the farm only reached {peak_util:.1f}% (< {MIN_UTIL}%).",
              flush=True)
    elif not contended:
        print("INCONCLUSIVE: a job ran out of waiting frames inside the window, so the "
              "split was not contended.", flush=True)
    elif share < MIN_HI_SHARE:
        print(f"FAIL: the priority-{pri.get('hijob', '?')} job holds {share:.0%} of the "
              f"contested cores (< {MIN_HI_SHARE:.0%}); the priority-{pri.get('lojob', '?')} "
              f"job holds {ratio:.1f}x as many. The slot draw is per layer and each layer "
              f"is capped to a quarter of every host, so layer count beats priority.",
              flush=True)
    else:
        print(f"PASS: the priority-{pri.get('hijob', '?')} job holds {share:.0%} of the "
              f"contested cores at {peak_util:.1f}% peak utilization.", flush=True)


if __name__ == "__main__":
    main()
