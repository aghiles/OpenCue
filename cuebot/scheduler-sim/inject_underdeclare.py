"""UNDERDECLARE workload: layers that declare 4G but really hold 18G of rss.

The other production disease (STRANDGROW's sibling). An artist declares 4G;
the frames balloon to 18G. Placement reserves the declared 4G, so cuebot
packs ~13 such frames on a 56G host, they all balloon, the host drowns in
swap and cuebot's host-OOM logic starts killing frames. Without live-rss
sizing every fresh wave repeats the massacre for all 4000 frames; with it
only the pre-evidence window bleeds, then placements reserve the true 18G
(and book the metric core share) and the kills stop.

The farm is 40 small hosts (the scenario pins --hosts 0,0,40): 4000 waiting
balloons against ~520 first-wave slots concentrate by arithmetic, so the
disease needs no background workload and no saturation phase. After the
ledger converges, the honest 500-point/18G shape legally fits three frames
per host and churns (durshort), so the late window proves convergence
with a steady flow of real completions.

Three jobs, one group (same tag), all declaring 4096mb:
  - flood1: threadable, asks 1 core.  Goes through the probe gate (max 8
            farm-wide before evidence), so its casualties are gate-bounded.
  - flood4: threadable, asks 4 cores. An explicit ask is deliberately never
            gated, so its first wave is the full unprotected OOM burst; only
            the rss ledger converging can stop it. Must end at 500 points
            (corrected upward, never stalled).
  - ctrl:   NON-threadable, honest rss (no pin). Must never be resized and
            never killed.

The fake RQD pins only the flood jobs' rss at 18G (SIM_RSS_PIN token
"simunderdecl_flood" matches flood1/flood4, not ctrl).

usage: inject_underdeclare.py [duration_s]
"""
import os, sys, time
import grpc
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import job_pb2, job_pb2_grpc
import farm_spec as spec

CUEBOT = spec.GRPC
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 240
FLOOD_FRAMES = int(os.environ.get("SIM_UNDERDECL_FRAMES", "2000"))
CTRL_FRAMES = int(os.environ.get("SIM_UNDERDECL_CTRL_FRAMES", "25"))
DECL_MB = int(os.environ.get("SIM_UNDERDECL_DECL_MB", "4096"))
TOKEN = "simunderdecl"

SPEC_HEAD = ('<?xml version="1.0"?>\n'
  '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
  '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
  '  <user>sim</user>\n  <uid>9860</uid>\n')


def job_xml(job, layer, frames, threadable, cores, mem_mb):
    lay = (f'      <layer name="{layer}" type="Render"><cmd>/bin/true</cmd>'
           f'<range>1-{frames}</range><chunk>1</chunk>'
           f'<cores>{cores}</cores>'
           f'<threadable>{threadable}</threadable><memory>{mem_mb}mb</memory>'
           f'<tags>{spec.TAG}</tags>'
           f'<services><service>shell</service></services></layer>')
    return (f'  <job name="sim-test-{TOKEN}-{job}-durshort-00001"><paused>false'
            f'</paused><priority>100</priority><maxcores>80000</maxcores>\n'
            f'    <layers>\n{lay}\n    </layers>\n  </job>\n')


def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    for body in (job_xml("flood1", "balloon1c", FLOOD_FRAMES, 1, 100, DECL_MB),
                 job_xml("flood4", "balloon4c", FLOOD_FRAMES, 1, 400, DECL_MB),
                 job_xml("ctrl", "honest", CTRL_FRAMES, 0, 100, DECL_MB)):
        stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=SPEC_HEAD + body
                                                     + "</spec>\n"))
    print(f"UNDERDECLARE: 2x{FLOOD_FRAMES} threadable frames declaring "
          f"{DECL_MB}mb (asks 1 and 4 cores) whose rss is pinned at 18G, "
          f"+ {CTRL_FRAMES} honest non-threadable controls, on 40 small "
          f"hosts, staying up {DURATION}s.", flush=True)
    t0 = time.time()
    while time.time() - t0 < DURATION:
        time.sleep(5)
    print("injector done", flush=True)


if __name__ == "__main__":
    main()
