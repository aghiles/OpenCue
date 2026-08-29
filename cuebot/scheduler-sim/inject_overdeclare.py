"""OVERDECLARE workload: layers that reserve far more memory than they use.

The mirror of UNDERDECLARE, and the legacy memory balancer's whole job.
Production fleets are full of layers that declare 16G and hold 2G: placement
reserves the declaration, hosts fill memory-first at a fraction of their
cores, and the farm packs to ~22% while honest work waits. Stock OpenCue
heals this from the completion path: balanceLayerMinMemory lowers a layer's
int_mem_min to its observed max rss plus 256M after successes (b_optimize
layers only, downward only). The batched drain bypassed that path, so under
the new scheduler nothing ever shrinks a lying declaration.

Full farm. Three arms, all pinned to a real 2G rss (SIM_RSS_PIN
simoverdecl=2 matches every arm's token):
  All arms carry durlong (120s fixed frames): churn per tick then stays far
under the report-path ceiling, so the memory disease shows as utilization,
not as booking-rate starvation.

  flood   sim-test-simoverdecl-*     declares 16G, uses 2G, b_optimize on:
                                     must shrink to ~2.3G after evidence and
                                     repack the farm cores-bound.
  control sim-test-simoverdeclctl-*  declares 2G, uses 2G, honest: must book
                                     normally the whole run and never shrink
                                     below its truthful ask.
  guard   sim-test-simoverdeclnoopt- declares 16G, uses 2G, b_optimize OFF
                                     (flipped in the DB after submit): must
                                     NEVER shrink; the flag is the contract.

Non-threadable on purpose: the scheduler's own rss resize reshapes only
threadable layers in memory, so any healing seen here is the balancer's.

usage: inject_overdeclare.py [duration_s]
"""
import os, subprocess, sys, time
import grpc
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import job_pb2, job_pb2_grpc
import farm_spec as spec

CUEBOT = spec.GRPC
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300
FRAMES = int(os.environ.get("SIM_OVERDECLARE_FRAMES", "800"))
JOBS = int(os.environ.get("SIM_OVERDECLARE_JOBS", "60"))
TOKEN = "simoverdecl"
PSQL = spec.psql_cmd()

SPEC_HEAD = ('<?xml version="1.0"?>\n'
  '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
  '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
  '  <user>sim</user>\n  <uid>9860</uid>\n')


def one_job(name, mem_mb):
    layer = (f'      <layer name="work" type="Render"><cmd>/bin/true</cmd>'
             f'<range>1-{FRAMES}</range><chunk>1</chunk>'
             f'<cores>100</cores>'
             f'<threadable>0</threadable><memory>{mem_mb}mb</memory>'
             f'<tags>{spec.TAG}</tags>'
             f'<services><service>shell</service></services></layer>')
    return (f'  <job name="{name}"><paused>false</paused>'
            f'<priority>100</priority><maxcores>80000</maxcores>\n'
            f'    <layers>\n{layer}\n    </layers>\n  </job>\n')


def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    for n in range(1, JOBS + 1):
        xml = SPEC_HEAD + one_job(f"sim-test-{TOKEN}-durlong-{n:05d}", 16384) + "</spec>\n"
        stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
    for n in range(1, 5):
        xml = SPEC_HEAD + one_job(f"sim-test-{TOKEN}ctl-durlong-{n:05d}", 2048) + "</spec>\n"
        stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
    for n in range(1, 3):
        xml = SPEC_HEAD + one_job(f"sim-test-{TOKEN}noopt-durlong-{n:05d}", 16384) + "</spec>\n"
        stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
    # The guard arm's contract is the b_optimize flag itself; there is no job
    # spec field for it, so flip it in the DB the way an admin call would.
    # LaunchSpec ingests asynchronously, so retry until the layers exist and
    # the update reports rows changed; a raced zero-row update would leave
    # the flag true and void the guard's contract.
    for _ in range(30):
        r = subprocess.run(PSQL + ["-c",
            "UPDATE layer SET b_optimize=false WHERE pk_job IN "
            f"(SELECT pk_job FROM job WHERE str_name LIKE '%{TOKEN}noopt%');"],
            capture_output=True, text=True, timeout=15)
        if "UPDATE 0" not in (r.stdout or "") and "UPDATE" in (r.stdout or ""):
            print(f"guard flag set: {r.stdout.strip()}", flush=True)
            break
        time.sleep(2)
    print(f"OVERDECLARE: {JOBS} lying jobs (16G declared, 2G real), 4 honest "
          f"controls, 2 no-optimize guards, staying up {DURATION}s.", flush=True)
    t0 = time.time()
    while time.time() - t0 < DURATION:
        time.sleep(5)
    print("injector done", flush=True)


if __name__ == "__main__":
    main()
