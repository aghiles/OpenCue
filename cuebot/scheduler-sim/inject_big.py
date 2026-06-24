"""Inject BIG (wide) jobs into a farm the small feeder keeps saturated, as a
*guaranteed* test of reservations: with reservations OFF these jobs can never
run; with reservations ON they do.

Why it's guaranteed. A work-conserving planner fills any idle core with ready
small work. With a deep 1-2 core backlog, cores free a few at a time and are
refilled within a tick, so a wide block of idle cores never accumulates on a
single host. A 64-core frame needs 64 idle cores at once on one host -- which
never happens while small work is waiting. The only escape is the cold-start
transient (an empty farm has whole idle hosts), so this injector WAITS until the
farm is saturated -- no host has a 64-core idle block -- before injecting. After
that, the only way 64 contiguous cores can ever open is the scheduler reserving
a host and draining it (the drain guard stops small work from refilling it). So:

  - reservations OFF  -> big frames strand forever (run=0).
  - reservations ON   -> the blocked layer reserves a jaime, it drains, big runs.

Width: cuebot clamps a layer's per-frame reservation to
Dispatcher.CORE_POINTS_RESERVED_MAX_NEW (6400 points = 64 cores) under the NEW
scheduler. We request exactly 64 so the spec matches what the scheduler reserves;
64 cores is half a 128-core jaime -- wide enough to qualify for a reservation
(>= 0.5 of the largest host in the group) yet within what one reservation holds.

Big frames request LOW memory (16 GB), trivially satisfiable, so the ONLY reason
a big frame can't run is core fragmentation -- never memory. Jobs are injected at
the SAME priority as the small stream: classic head-of-line starvation, where the
continuously-fed narrow work always wins the transient gaps, so the wide frame
never accumulates its cores. (Equal priority is what makes the starvation
guaranteed: a HIGHER-priority big job is NOT guaranteed to strand -- under fast
churn it can opportunistically grab a transient >64-core gap before the small
work refills it. So we inject at equal priority.)

usage: inject_big.py [duration_s] [interval_s]
"""
import os, sys, time, subprocess
import grpc
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import job_pb2, job_pb2_grpc
import sim_model
import farm_spec as spec

CUEBOT = spec.GRPC
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300
INTERVAL = int(sys.argv[2]) if len(sys.argv) > 2 else 30

BIG_CORES  = 64                       # whole cores per frame (NEW reservation clamp)
BIG_MEM_MB = 16 * 1024                # low: memory is never the binding reason
BIG_FRAMES = 24                       # frames per big job
PRI = 1                               # SAME priority as the small feeder jobs
SATURATE_TIMEOUT = 240                # s to wait for the farm to fill before giving up

PSQL = spec.psql_cmd()

SPEC_HEAD = ('<?xml version="1.0"?>\n'
  '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
  '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
  '  <user>sim</user>\n  <uid>9860</uid>\n')


def max_host_idle_cores():
    """Largest idle-core block (whole cores) on any single host right now, or -1
    on error. While this is >= BIG_CORES some host could trivially fit a big
    frame, so injecting then would not test reservations."""
    try:
        out = subprocess.run(
            PSQL + ["-c", "SELECT COALESCE(MAX(int_cores_idle),0) FROM host;"],
            capture_output=True, text=True, timeout=10).stdout.strip()
        return int(out) // sim_model.CORE_POINTS
    except Exception:
        return -1


def wait_for_saturation():
    """Block until no host has a BIG_CORES idle block (the farm is full of small
    work), so an injected big frame can only ever run via a reservation. Returns
    True if saturation was reached, False if it timed out."""
    t0 = time.time()
    while time.time() - t0 < SATURATE_TIMEOUT:
        m = max_host_idle_cores()
        if 0 <= m < BIG_CORES:
            print(f"t={time.time()-t0:5.0f}s farm SATURATED (max host idle "
                  f"{m} < {BIG_CORES} cores) -- a big frame can now only run via "
                  f"a reservation; injecting", flush=True)
            return True
        print(f"t={time.time()-t0:5.0f}s waiting for saturation "
              f"(max host idle = {m} cores, need < {BIG_CORES}) ...", flush=True)
        time.sleep(3)
    print(f"WARN farm did not saturate within {SATURATE_TIMEOUT}s "
          f"(max host idle still >= {BIG_CORES}); the test is NOT guaranteed",
          flush=True)
    return False


def make_big(name, pri):
    cores_pts = BIG_CORES * sim_model.CORE_POINTS
    layer = (f'      <layer name="big" type="Render">'
        f'<cmd>/bin/true</cmd><range>1-{BIG_FRAMES}</range><chunk>1</chunk>'
        f'<cores>{cores_pts}</cores><threadable>{sim_model.THREADABLE}</threadable>'
        f'<memory>{BIG_MEM_MB}mb</memory>'
        f'<tags>{spec.TAG}</tags><services><service>shell</service></services></layer>')
    # priority comes right after paused per the cjsl DTD; maxcores generous so
    # it is never the reason a frame can't book.
    return (f'  <job name="{name}"><paused>false</paused><priority>{pri}</priority>'
            f'<maxcores>{cores_pts * BIG_FRAMES}</maxcores>\n'
            '    <layers>\n' + layer + "\n    </layers>\n  </job>\n")


def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    # Gate: only inject once the farm is saturated, so a big frame strands unless
    # a reservation drains a host for it. This is what makes the test guaranteed.
    wait_for_saturation()
    t0 = time.time(); n = 0
    while time.time() - t0 < DURATION:
        n += 1
        # Keep the 'big_eq' token: live_stats.py / strand_watch.py match big jobs
        # by the stable big_eq / big_hi name tokens. All equal priority now, so
        # they all land in the BIG[equal] class (BIG[high] stays empty).
        name = f"sim-test-big-eq-{n:03d}"
        xml = SPEC_HEAD + make_big(name, PRI) + "</spec>\n"
        try:
            stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
            print(f"t={time.time()-t0:5.0f}s injected {name} "
                  f"pri={PRI} cores={BIG_CORES} frames={BIG_FRAMES}", flush=True)
        except grpc.RpcError as e:
            print(f"t={time.time()-t0:5.0f}s inject FAILED: {e}", flush=True)
        # Sleep to the next interval without overshooting the duration.
        end = min(t0 + DURATION, time.time() + INTERVAL)
        while time.time() < end:
            time.sleep(min(2.0, end - time.time()))
    print(f"injector done: submitted {n} big jobs", flush=True)


if __name__ == "__main__":
    main()
