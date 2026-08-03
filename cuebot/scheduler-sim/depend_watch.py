"""DEPENDS (dependency-correctness) verdict: does work ever run before its parents?

The feeder submits every unit of work as a JOB_ON_JOB dependency tree (see
feed.py): only the root is runnable at submit; descendants sit in DEPEND until
cuebot satisfies the depend when the parent finishes. This watcher asserts the
one invariant that must NEVER break, plus proves the machinery actually cycled:

  INVARIANT (sampled continuously): no frame is ever RUNNING while it still has
  unsatisfied depends (frame.int_depend_count > 0). The dispatcher only picks
  WAITING frames and a frame only becomes WAITING when its count hits 0, so a
  single violation means a frame ran before its parents completed.

  COVERAGE (start-vs-end deltas, so the verdict can't pass vacuously):
    - depends satisfied during the watch (depend.b_active flips false) >= floor
    - frames STARTED in depend-er jobs (work that was gated and then ran) >= floor

  - PASS: zero violations across every sample AND both coverage floors met.
  - INCONCLUSIVE: coverage floors not met (no dependency traffic to judge).
  - FAIL: any sample caught a RUNNING frame with int_depend_count > 0.

usage: depend_watch.py [duration_s] [interval_s]
"""
import os, sys, time, subprocess
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import farm_spec as spec

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 180
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0
PSQL = spec.psql_cmd()
MIN_SATISFIED = int(os.environ.get("SIM_DEPEND_MIN_SATISFIED", "10"))
MIN_STARTED = int(os.environ.get("SIM_DEPEND_MIN_STARTED", "200"))


def _scalar(sql, default=0):
    """Run a single-value query and return it as an int, or `default`.

    Any failure collapses to `default` so a transient DB hiccup costs one
    sample rather than the whole watch.
    """
    try:
        out = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True,
                             timeout=15).stdout.strip()
        return int(out) if out.lstrip("-").isdigit() else default
    except Exception:
        return default


def violations():
    """Count frames running despite unsatisfied depends -- the correctness bug.

    Any value above zero, at any single sample, fails the scenario outright:
    this is a safety property, so unlike the throughput metrics it is never
    averaged or given a tolerance.
    """
    return _scalar("SELECT count(*) FROM frame "
                   "WHERE str_state='RUNNING' AND int_depend_count > 0;")


def satisfied():
    """Count depends that have been satisfied and deactivated so far."""
    return _scalar("SELECT count(*) FROM depend WHERE b_active=false;")


def depend_total():
    """Count depend records in the database, satisfied or not."""
    return _scalar("SELECT count(*) FROM depend;")


def depender_started():
    """Frames that actually STARTED inside depend-er jobs: gated work that ran."""
    return _scalar("SELECT count(*) FROM frame f WHERE f.ts_started IS NOT NULL "
                   "AND f.pk_job IN (SELECT pk_job_depend_er FROM depend);")


def depend_frames():
    """Count frames currently held in the DEPEND state."""
    return _scalar("SELECT count(*) FROM frame WHERE str_state='DEPEND';")


def main():
    """Sample dependency correctness for DURATION and print a verdict.

    Combines a safety check with two coverage floors. The safety check -- no
    frame ever RUNNING with a non-zero depend count -- is what can fail the
    run. The floors exist because that check passes trivially on a run where
    nothing was ever gated, so the scenario also requires that depends were
    actually satisfied and that gated frames actually started afterward.
    """
    print(f"watching DEPENDS for {DURATION}s: no frame may ever RUN with "
          f"unsatisfied depends; coverage floors satisfied>={MIN_SATISFIED}, "
          f"gated-frames-started>={MIN_STARTED}.\n", flush=True)
    t0 = time.time()
    sat0 = satisfied()
    started0 = depender_started()
    viol_samples = 0
    viol_peak = 0
    while time.time() - t0 < DURATION:
        t = time.time() - t0
        v = violations()
        dep = depend_frames()
        sat = satisfied() - sat0
        started = depender_started() - started0
        if v > 0:
            viol_samples += 1
            viol_peak = max(viol_peak, v)
        flag = "  <-- VIOLATION" if v > 0 else ""
        print(f"t={t:5.0f} | RUNNING-with-depends {v:3d} | DEPEND backlog {dep:6d} "
              f"| satisfied +{sat:5d} | gated started +{started:6d}{flag}", flush=True)
        time.sleep(INTERVAL)

    sat = satisfied() - sat0
    started = depender_started() - started0
    total = depend_total()
    print("\n==== DEPENDS VERDICT ====", flush=True)
    print(f"violation samples={viol_samples} (peak {viol_peak})  "
          f"depends total={total} satisfied during watch={sat}  "
          f"gated frames started={started}", flush=True)
    if viol_samples > 0:
        print(f"FAIL: caught frames RUNNING with unsatisfied depends in "
              f"{viol_samples} samples (peak {viol_peak} at once) -- work ran "
              f"before its parents completed.", flush=True)
    elif sat < MIN_SATISFIED or started < MIN_STARTED:
        print(f"INCONCLUSIVE: not enough dependency traffic to judge "
              f"(satisfied {sat} < {MIN_SATISFIED} or gated-started {started} < "
              f"{MIN_STARTED}). Raise load/duration.", flush=True)
    else:
        print(f"PASS: {sat} depends satisfied and {started} previously-gated "
              f"frames ran, with zero RUNNING-with-depends violations -- the "
              f"scheduler respects dependencies.", flush=True)


if __name__ == "__main__":
    main()
