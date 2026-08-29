"""SQUEEZE workload: wide memory-heavy frames vs hosts that almost fit them.

The stranding disease at frame granularity, on the FULL farm (production
shape mix). Many shots of the same heavy work: threadable 16-core/56G
layers (3.5G per core). Larges and mediums pack completely, but the small
tier (16c hosts, 65% of the farm's machines) misses the request by half a
gigabyte of usable memory and can never fit it. Without a reduction that
whole tier strands: utilization plateaus near 72% with a deep backlog
waiting. With it the scheduler may book a frame at down to 80% of the
requested cores when the score finds no better placement. Memory is not
reduced with the cores: the booking reserves the layer's observed use, or
the declared figure pro rated to the reduced count when that is higher, so
a 15-core/52.5G frame fits the small host and the tier goes back to work.

The job name carries sqzwork16: fake_rqd scales each frame's duration by
granted cores (base * 16 / granted), so squeezed frames run proportionally
longer and the verdict must prove a NET throughput win, not a free lunch.

usage: inject_squeeze.py [duration_s]
"""
import os, sys, time
import grpc
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import job_pb2, job_pb2_grpc
import farm_spec as spec

CUEBOT = spec.GRPC
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300
FRAMES = int(os.environ.get("SIM_SQUEEZE_FRAMES", "600"))
JOBS = int(os.environ.get("SIM_SQUEEZE_JOBS", "100"))
TOKEN = "simsqueeze"

SPEC_HEAD = ('<?xml version="1.0"?>\n'
  '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
  '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
  '  <user>sim</user>\n  <uid>9860</uid>\n')


def make_job(n):
    # 16 cores / 56G declared (3.5G per core), threadable. The rss pin
    # (SIM_RSS_PIN simsqueeze=52, set by the scenario) makes every frame
    # really hold 52G under its 56G declaration: the realistic over-declared
    # wide layer. The squeeze floors its memory at that evidence, so the
    # 15-core slice reserves 52.5G and the true footprint always fits it.
    layer = (f'      <layer name="wide" type="Render"><cmd>/bin/true</cmd>'
             f'<range>1-{FRAMES}</range><chunk>1</chunk>'
             f'<cores>1600</cores>'
             f'<threadable>1</threadable><memory>57344mb</memory>'
             f'<tags>{spec.TAG}</tags>'
             f'<services><service>shell</service></services></layer>')
    return (f'  <job name="sim-test-{TOKEN}-sqzwork16-{n:05d}"><paused>false</paused>'
            f'<priority>100</priority><maxcores>80000</maxcores>\n'
            f'    <layers>\n{layer}\n    </layers>\n  </job>\n')


def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    for n in range(1, JOBS + 1):
        xml = SPEC_HEAD + make_job(n) + "</spec>\n"
        stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
    print(f"SQUEEZE: {JOBS} jobs x {FRAMES} threadable 16-core/56G frames on "
          f"the full farm, staying up {DURATION}s.", flush=True)
    t0 = time.time()
    while time.time() - t0 < DURATION:
        time.sleep(5)
    print("injector done", flush=True)


if __name__ == "__main__":
    main()
