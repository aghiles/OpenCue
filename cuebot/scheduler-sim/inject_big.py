"""Inject BIG (wide) jobs into a farm that the small feeder keeps busy.

Demonstrates the cost of E-PVM load-balancing WITHOUT reservations: jaime (the
only 128-core class that can comfortably fit a wide frame) fills with the
small-frame stream, and when small frames finish they free a few cores at a
time which the planner immediately refills with more small frames -- so the
wide block of idle cores never accumulates on a single host and the big frame
waits forever.

Note on width: cuebot clamps a layer's per-frame reservation to
Dispatcher.CORE_POINTS_RESERVED_MAX_NEW (6400 points = 64 cores) when the NEW
scheduler is enabled (the legacy cap is 2400 = 24 cores). We request 64 so the
spec matches what the scheduler reserves -- 64 contiguous cores on a 128-core
jaime is genuinely wide (half the host) and strands hard under churn.

Big frames request LOW memory (16 GB), so memory is trivially satisfiable on
jaime: the ONLY reason a big frame can't run is core fragmentation. Jobs are
injected "once in a while" (every INTERVAL seconds), alternating priority so a
single run shows both:
  - EQUAL priority (same as small work): ordinary head-of-line starvation.
  - HIGH  priority (well above small work): proves urgency alone can't rescue a
    big job without a reservation to hold cores for it.

usage: inject_big.py [duration_s] [interval_s]
"""
import os, sys, time
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

BIG_CORES  = 64                       # whole cores per frame (NEW clamp max)
BIG_MEM_MB = 16 * 1024                # low: memory is never the binding reason
BIG_FRAMES = 24                       # frames per big job
EQUAL_PRI  = 1                        # same as the small feeder jobs
HIGH_PRI   = 10000                    # clearly above the small work

SPEC_HEAD = ('<?xml version="1.0"?>\n'
  '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
  '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
  '  <user>sim</user>\n  <uid>9860</uid>\n')


def make_big(name, pri):
    cores_pts = BIG_CORES * sim_model.CORE_POINTS
    layer = (f'      <layer name="big" type="Render">'
        f'<cmd>/bin/true</cmd><range>1-{BIG_FRAMES}</range><chunk>1</chunk>'
        f'<cores>{cores_pts}</cores><threadable>1</threadable>'
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
    t0 = time.time(); n = 0
    while time.time() - t0 < DURATION:
        n += 1
        equal = (n % 2 == 1)
        pri = EQUAL_PRI if equal else HIGH_PRI
        cls = "eq" if equal else "hi"
        name = f"sim-test-big-{cls}-{n:03d}"
        xml = SPEC_HEAD + make_big(name, pri) + "</spec>\n"
        try:
            stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
            print(f"t={time.time()-t0:5.0f}s injected {name} "
                  f"pri={pri} cores={BIG_CORES} frames={BIG_FRAMES}", flush=True)
        except grpc.RpcError as e:
            print(f"t={time.time()-t0:5.0f}s inject FAILED: {e}", flush=True)
        # Sleep to the next interval without overshooting the duration.
        end = min(t0 + DURATION, time.time() + INTERVAL)
        while time.time() < end:
            time.sleep(min(2.0, end - time.time()))
    print(f"injector done: submitted {n} big jobs", flush=True)


if __name__ == "__main__":
    main()
