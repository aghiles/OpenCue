"""LICENSE test: does the planner respect LIVE license availability?

Companion to fake_license.py (the license server) and license_watch.py (the
verdict). Floods the farm with work that needs application licenses, declared
the way a submitter declares them -- in the layer's environment:

    <env><key name="CUE_LICENSES">katana,maya</key></env>

and cuebot gates placement on what the license server says is free RIGHT NOW,
not on a number an admin typed. The backlog is far deeper than any pool, so the
licenses are the only thing that can hold concurrency down.

Five layer flavours, so every path in the feature is exercised at once:

  hengine        host-based pool: the cap counts distinct MACHINES and frames on
                 a seated machine share its checkout, so this should pack onto
                 few hosts and run many frames.
  katana         floating pool with headroom on the cuebot side.
  maya           floating pool, no headroom.
  katana,maya    needs a seat in BOTH (AND, not OR) -- gated by whichever is
                 tighter, and consuming one of each when it runs.
  (none)         unlicensed control. Must be completely unaffected: if these
                 stop running, the feature is gating work it has no business
                 touching.

usage: inject_license.py [duration_s]
"""
import os
import random
import subprocess
import sys
import time

import grpc

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import job_pb2
import job_pb2_grpc
import sim_model
import farm_spec as spec

CUEBOT = spec.GRPC
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 180
ENV_KEY = os.environ.get("SIM_LIC_ENV_KEY", "CUE_LICENSES")

# Layer flavours and their weights. Licensed work dominates so the pools are
# under real pressure, with a solid slice of unlicensed work as the control.
FLAVOURS = [
    ("hengine", 3),
    ("katana", 3),
    ("maya", 2),
    ("katana,maya", 2),
    ("", 3),
]
# Keep a deep runnable backlog: many more frames want a license than any pool
# has, so a scheduler that ignores availability will visibly overshoot.
TARGET = int(os.environ.get("SIM_LIC_TARGET", "6000"))
WAVE = 6                              # specs per burst (launch queue is bounded)
LAYERS_MIN, LAYERS_MAX = 3, 6
FRAMES_MIN, FRAMES_MAX = 60, 120      # long-ish, so concurrency accumulates
PSQL = spec.psql_cmd()

SPEC_HEAD = ('<?xml version="1.0"?>\n'
             '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
             '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
             '  <user>sim</user>\n  <uid>9860</uid>\n')

TOKEN = "simlicense"


def _pick(rng):
    """Draw one license flavour from FLAVOURS by weight.

    The mix includes an unlicensed entry on purpose: it becomes the control
    group the watcher checks is entirely unaffected by license pressure.
    """
    names = [f for f, _ in FLAVOURS]
    weights = [w for _, w in FLAVOURS]
    return rng.choices(names, weights=weights, k=1)[0]


def make_job(name, rng):
    """Build the job spec XML for one job with per-layer license requirements.

    Each layer draws its own flavour, so a single job can span several licenses
    and the unlicensed control -- which is what forces the scheduler to gate
    per layer rather than per job.
    """
    n = rng.randint(LAYERS_MIN, LAYERS_MAX)
    layers = []
    for li in range(n):
        nf = rng.randint(FRAMES_MIN, FRAMES_MAX)
        lic = _pick(rng)
        # DTD order: cmd,range,chunk,cores,threadable,memory,...,tags,limits,env,services
        env = (f'<env><key name="{ENV_KEY}">{lic}</key></env>' if lic else "")
        # 1 core per frame = 1 license unit, and cores never bind before the
        # licenses do.
        layers.append(
            f'      <layer name="lyr{li}" type="Render"><cmd>/bin/true</cmd>'
            f'<range>1-{nf}</range><chunk>1</chunk>'
            f'<cores>{sim_model.CORE_POINTS}</cores>'
            f'<threadable>0</threadable><memory>512mb</memory>'
            f'<tags>{spec.TAG}</tags>'
            f'{env}'
            f'<services><service>shell</service></services></layer>')
    return (f'  <job name="{name}"><paused>false</paused><priority>100</priority>'
            f'<maxcores>80000</maxcores>\n    <layers>\n'
            + "\n".join(layers) + "\n    </layers>\n  </job>\n")


def _scalar(sql, cast, default):
    """Run a single-value query and return it through `cast`, or `default`."""
    try:
        out = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True,
                             timeout=10).stdout.strip()
        return cast(out)
    except Exception:
        return default


def waiting():
    """Depend-free WAITING frames of this test's jobs (the live runnable backlog)."""
    return _scalar(f"SELECT count(*) FROM frame f JOIN job j ON f.pk_job=j.pk_job "
                   f"WHERE j.str_name LIKE '%{TOKEN}%' AND f.str_state='WAITING' "
                   f"AND f.int_depend_count=0;", int, -1)


def licensed_running():
    """RUNNING frames that hold at least one license (any pool)."""
    return _scalar(f"SELECT count(*) FROM frame f "
                   f"JOIN layer_env le ON le.pk_layer = f.pk_layer "
                   f"WHERE le.str_key='{ENV_KEY}' AND le.str_value <> '' "
                   f"AND f.str_state='RUNNING';", int, -1)


def util_pct():
    """Farm utilization from the host table's own core accounting."""
    return _scalar("SELECT COALESCE(100.0*(sum(int_cores)-sum(int_cores_idle))"
                   "/NULLIF(sum(int_cores),0),0) FROM host;", float, -1.0)


def submit_wave(stub, prefix, seq):
    """Launch up to WAVE jobs, returning the sequence number reached.

    Stops the wave and backs off on a gRPC rejection, which means cuebot's
    launch queue is full.
    """
    for _ in range(WAVE):
        seq += 1
        xml = SPEC_HEAD + make_job(f"{prefix}-{seq:05d}", random.Random(seq * 13)) + "</spec>\n"
        try:
            stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
        except grpc.RpcError:
            time.sleep(2.0)            # launch queue full -> back off, retry next tick
            break
    return seq


def main():
    """Hold the farm oversupplied with license-bound work for DURATION.

    Demand has to exceed the available seats continuously for the scheduler's
    gating to be under test at all, so the queue is topped back up to TARGET
    each tick rather than submitted as one burst.
    """
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    mix = ", ".join(f"{f or '(unlicensed)'}x{w}" for f, w in FLAVOURS)
    print(f"LICENSE test: flood the farm with license-bound layers ({mix}), "
          f"hold {TARGET} waiting, for {DURATION}s.", flush=True)
    t0 = time.time()
    seq = 0
    while time.time() - t0 < DURATION:
        w = waiting()
        if 0 <= w < TARGET:
            seq = submit_wave(stub, f"sim-test-{TOKEN}", seq)
        print(f"t={time.time()-t0:5.0f}s util={util_pct():5.1f}% waiting={w} "
              f"licensedRunning={licensed_running()} submitted={seq}", flush=True)
        time.sleep(2.0)
    print(f"injector done: submitted {seq} jobs", flush=True)


if __name__ == "__main__":
    main()
