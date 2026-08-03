#!/usr/bin/env python
"""Quick utilization regression test. Run at every commit.

Brings up a fresh full-farm sim, lets it fill, then measures REAL utilization =
cores backing actually-running frames / total farm cores (proc.pk_frame NOT NULL
joined to RUNNING frames -- so leaked/orphaned procs do NOT count). Prints a one
line PASS/FAIL and exits non-zero on FAIL so CI / a commit hook can gate on it.

usage:
    util_test.py [--mode new|old] [--fill SECONDS] [--threshold PCT]
                 [--compress F] [--target N]

defaults: --mode new --fill 200 --threshold 50 --compress 8 --target 40000
"""
import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import farm_spec as spec

JAVA_HOME = os.environ.get("JAVA_HOME", "/tmp/jdk-17.0.2")
VENV_PY = os.path.join(HERE, "venv", "bin", "python")
SIM_LOG = "/tmp/util_test_sim.log"

# One query: farm cores, cores backing a RUNNING frame, orphaned (no-frame)
# procs, and running-frame count. pk_frame links a proc to the frame it runs;
# NULL means the proc holds cores but does no work (a leak).
SQL = (
    "SELECT (SELECT sum(int_cores) FROM host), "
    "(SELECT coalesce(sum(int_cores_reserved),0) FROM proc WHERE pk_frame IS NOT NULL), "
    "(SELECT count(*) FROM proc WHERE pk_frame IS NULL), "
    "(SELECT count(*) FROM frame WHERE str_state='RUNNING');"
)


def measure():
    """Return (honest_util_pct, running_frames, leaked_procs).

    Utilization counts only cores held by procs that back a running frame.
    Procs with a NULL pk_frame hold cores while doing no work, so including
    them would let a leaking scheduler report high utilization while the farm
    accomplishes nothing -- they are returned separately as the leak signal.
    """
    out = subprocess.run(spec.psql_cmd(tab=True) + ["-c", SQL],
                         capture_output=True, text=True, timeout=30).stdout.strip()
    farm, with_frame, procs_no_frame, running = [int(x) for x in out.split("\t")]
    util = 100.0 * with_frame / farm if farm else 0.0
    return util, running, procs_no_frame


def kill_stack():
    """Tear down any leftover sim stack so a run starts from a clean slate.

    Matches cuebot, the gradle daemon and the harness helpers by process name.
    The patterns are chosen not to match this script itself, which would make
    the teardown kill the run it is preparing for.
    """
    subprocess.run(
        "ps -eo pid,args | grep -iE 'CuebotApplication|bootRun|GradleDaemon|"
        "fake_rqd|rqd_report|feed.py|simulate.py' | grep -v grep | "
        "awk '{print $1}' | xargs -r kill -9",
        shell=True)
    time.sleep(3)


def main():
    """Drive a sim run to a target utilization and report whether it got there.

    Brings up the stack under the requested scheduler mode, feeds it until
    utilization crosses --threshold, and reports the honest utilization
    alongside the leaked-proc count -- the two have to be read together, since
    a leak inflates any utilization figure that counts booked cores.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["new", "old"], default="new")
    ap.add_argument("--fill", type=int, default=200)
    ap.add_argument("--threshold", type=float, default=50.0)
    ap.add_argument("--compress", type=float, default=8)
    ap.add_argument("--target", type=int, default=40000)
    args = ap.parse_args()

    kill_stack()
    env = dict(os.environ, JAVA_HOME=JAVA_HOME)
    sim = subprocess.Popen(
        [VENV_PY, "simulate.py", "--mode", args.mode, "--reporter-threads", "64",
         "--compress", str(args.compress), "--feed", str(args.fill + 120),
         "--feed-target", str(args.target), "--heartbeat-interval", "2"],
        cwd=HERE, env=env,
        stdout=open(SIM_LOG, "w"), stderr=subprocess.STDOUT)

    print(f"[util_test] mode={args.mode} fill={args.fill}s threshold={args.threshold}%"
          f" -- bringing up farm (log {SIM_LOG}) ...", flush=True)

    # Wait for the stack to come up (or fail).
    t0 = time.time()
    while True:
        try:
            log = open(SIM_LOG).read()
        except FileNotFoundError:
            log = ""
        if "stack is UP" in log:
            break
        if "Traceback" in log or "BUILD FAILED" in log:
            print("[util_test] FAIL: stack failed to start:\n" + log[-800:])
            kill_stack()
            return 2
        if time.time() - t0 > 400:
            print("[util_test] FAIL: stack did not come up within 400s")
            kill_stack()
            return 2
        time.sleep(5)

    print(f"[util_test] stack up; filling for {args.fill}s ...", flush=True)
    time.sleep(args.fill)

    util, running, no_frame = measure()
    verdict = "PASS" if util >= args.threshold else "FAIL"
    print(f"[util_test] {verdict}  mode={args.mode}  UTIL={util:.0f}%  "
          f"(threshold {args.threshold:.0f}%)  framesRunning={running}  "
          f"procsNoFrame={no_frame}", flush=True)

    kill_stack()
    return 0 if util >= args.threshold else 1


if __name__ == "__main__":
    sys.exit(main())
