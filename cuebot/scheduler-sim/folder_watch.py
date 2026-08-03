"""FOLDER (group/dept core-cap) verdict: did the folder's running cores exceed
folder_resource.int_max_cores?

Companion to inject_folder.py. Samples the sim default folder's concurrent
running cores (ground truth: SUM of job_resource.int_cores over the folder's jobs)
and tracks the peak. A scheduler that honors the cap holds this at <= the cap no
matter how deep the backlog is.

  - PASS: peak running cores <= cap AND the backlog wanted more (so the cap, not a
    lack of work, held it). The scheduler enforces the folder cap.
  - INCONCLUSIVE: the backlog never pushed demand past the cap.
  - FAIL: peak running cores > cap -- the scheduler ignored the folder ceiling.

Units are core-points internally (100 = 1 core); reported in CORES.

usage: folder_watch.py [duration_s] [interval_s]
"""
import os, sys, time, subprocess
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import farm_spec as spec
import sim_metrics

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 180
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
PSQL = spec.psql_cmd()
CP = 100                                  # core-points per core
CSV = os.environ.get("SIM_FOLDER_CSV", "")


def _int(sql, default=0):
    """Run a single-value query and return it as an int, or `default`.

    Any failure -- timeout, psql error, non-numeric output -- collapses to
    `default` so a transient DB hiccup costs one sample, not the whole watch.
    """
    try:
        out = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True,
                             timeout=15).stdout.strip()
        return int(out) if out.lstrip("-").isdigit() else default
    except Exception:
        return default


FOLDER = subprocess.run(
    PSQL + ["-c", "SELECT pk_folder FROM folder WHERE b_default=true AND pk_show="
                  "(SELECT pk_show FROM show WHERE str_name='sim');"],
    capture_output=True, text=True, timeout=15).stdout.strip()


def cap_cores():
    """The folder's max-cores ceiling, in whole cores, or -1 if unset.

    Stored as core-points, so it is divided down here to match the units every
    other figure in this watcher reports. -1 passes through untouched: the
    injector sets the cap shortly after the watcher starts, and the loop
    re-reads until it appears.
    """
    cp = _int(f"SELECT int_max_cores FROM folder_resource WHERE pk_folder='{FOLDER}';", -1)
    return cp // CP if cp > 0 else cp


def folder_cores():
    """Running cores in the folder = SUM(job_resource.int_cores) over its jobs."""
    cp = _int(f"SELECT COALESCE(SUM(jr.int_cores),0) FROM job_resource jr "
              f"JOIN job j ON jr.pk_job=j.pk_job WHERE j.pk_folder='{FOLDER}';")
    return cp // CP


def waiting_cores():
    """Cores the folder's queued frames are asking for, in whole cores.

    This is the demand side of the cap check: holding running cores under the
    ceiling proves nothing unless there was queued work that wanted to exceed
    it, so the verdict reports INCONCLUSIVE when this never outgrows the cap.
    """
    cp = _int(f"SELECT COALESCE(SUM(l.int_cores_min * ls.int_waiting_count),0) "
              f"FROM layer l JOIN layer_stat ls ON ls.pk_layer=l.pk_layer "
              f"JOIN job j ON l.pk_job=j.pk_job WHERE j.pk_folder='{FOLDER}';")
    return cp // CP


def main():
    """Sample folder core usage for DURATION and print a cap verdict.

    Tracks the peak of running cores against the folder's ceiling, alongside
    the waiting-core backlog that establishes there was demand to cap.
    """
    N = cap_cores()
    print(f"watching FOLDER {FOLDER[:8]} (cap {N} cores) for {DURATION}s: running "
          f"cores must stay <= cap.\n", flush=True)
    t0 = time.time()
    peak_run = 0
    peak_wait = 0
    rows = []
    while time.time() - t0 < DURATION:
        nc = cap_cores()             # re-read: the injector sets the cap just after t=0
        if nc > 0:
            N = nc
        run = folder_cores()
        wait = waiting_cores()
        util, _ = sim_metrics.farm_state()
        peak_run = max(peak_run, run)
        peak_wait = max(peak_wait, wait)
        t = time.time() - t0
        rows.append((t, run, wait, util))
        flag = "  <-- OVER CAP" if N > 0 and run > N else ""
        print(f"t={t:5.0f} | util {util:5.1f}% | folder running {run:5d} / cap {N} "
              f"cores | waiting {wait:6d}{flag}", flush=True)
        time.sleep(INTERVAL)

    if CSV:
        try:
            with open(CSV, "w") as f:
                f.write("t,running_cores,waiting_cores,util,cap_cores\n")
                for (t, run, wait, util) in rows:
                    f.write(f"{t:.0f},{run},{wait},{util:.1f},{N}\n")
        except Exception as e:
            print(f"(could not write CSV {CSV}: {e})", flush=True)

    print("\n==== FOLDER VERDICT ====", flush=True)
    print(f"folder {FOLDER[:8]} cap={N} cores  peak running={peak_run} cores  "
          f"peak waiting={peak_wait} cores", flush=True)
    if N < 0:
        print("INCONCLUSIVE: folder has no cap (int_max_cores=-1).", flush=True)
    elif peak_wait <= N:
        print(f"INCONCLUSIVE: backlog never exceeded the cap (peak waiting "
              f"{peak_wait} <= {N} cores); nothing to cap.", flush=True)
    elif peak_run > N:
        print(f"FAIL: folder running cores peaked at {peak_run}, OVER the cap of "
              f"{N} -- the scheduler ignored the folder ceiling.", flush=True)
    else:
        print(f"PASS: folder running cores peaked at {peak_run} <= cap {N} while "
              f"{peak_wait} cores waited -- the scheduler held the folder cap.",
              flush=True)


if __name__ == "__main__":
    main()
