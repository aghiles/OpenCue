"""SQUEEZE_FLAT workload: the squeeze vs a layer whose memory does not shrink.

The squeeze may book a frame at down to 80% of the layer's cores. The
memory reservation must NOT follow the cores down. A scene-load-bound
render holds the same footprint at 13 cores as at 14, so a reservation
that shrank with the cores would under-reserve it. On a packed host the
overrun lands on physical memory: the host tips over, the host-OOM
balancer kills, the frame requeues, and the next tick places it on the
next such host. The scheduler instead reserves the observed use, and this
scenario proves that rule.

Twelve small hosts (16c/56G, 3.5G per usable core), three classes, tenants
placed first. The flood declares 14 cores/50G so the rss resize confirms its
shape instead of growing it (round(50G/3.5G) = 14), and the geometry stays
fixed:
  tena  4x  1-core/7G non-threadable  leaves 15c/48.5G: a MEMORY-short hole
        (50G does not fit; 46.4G scaled at 13 cores does).
  tenb  4x  3-core/2G threadable      leaves 13c/53.5G: a CORES-short hole
        (13 cores sit inside [80%, 100%) of the 14-core ask).
  seed  4x empty                      full 14c/50G fits: the four seed
        frames are the rss evidence (rssProven) and the true-50G live folds.

floodX: threadable 14c/50G frames pinned to a FLAT 50G rss
(SIM_RSS_PIN simfltx=50), short frames (the seeds and safe squeezes
complete and keep the throughput floor honest; a memory-short kill fires
within seconds, long before any completion). On the unmodified build the tena
squeeze reserves 46.4G, the frame holds 50G, tenant 7G + flood 50G = 57G on
a 56G host: swap spills, the host-OOM balancer kills (the flood frame holds
the swap, rss-proportional), the frame requeues, the next tick squeezes the
reopened hole: the kill cycle (the disease). The tenb squeeze is safe in
every version: 2G + 50G = 52G fits. After the fix (squeeze memory floored
at the rss evidence) tena holes must stay un-squeezed and kill-free, tenb
must keep booking, and the layer ask must never ratchet.

usage: inject_squeeze_flat.py [duration_s]
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
FLOOD_FRAMES = int(os.environ.get("SIM_SQZFLAT_FRAMES", "300"))
PSQL = spec.psql_cmd()

SPEC_HEAD = ('<?xml version="1.0"?>\n'
  '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
  '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
  '  <user>sim</user>\n  <uid>9860</uid>\n')


def one_job(name, cores_pts, mem_mb, threadable, frames):
    layer = (f'      <layer name="work" type="Render"><cmd>/bin/true</cmd>'
             f'<range>1-{frames}</range><chunk>1</chunk>'
             f'<cores>{cores_pts}</cores>'
             f'<threadable>{1 if threadable else 0}</threadable>'
             f'<memory>{mem_mb}mb</memory>'
             f'<tags>{spec.TAG}</tags>'
             f'<services><service>shell</service></services></layer>')
    return (f'  <job name="{name}"><paused>false</paused>'
            f'<priority>100</priority><maxcores>80000</maxcores>\n'
            f'    <layers>\n{layer}\n    </layers>\n  </job>\n')


def launch(stub, name, cores_pts, mem_mb, threadable, frames=1):
    xml = SPEC_HEAD + one_job(name, cores_pts, mem_mb, threadable, frames) + "</spec>\n"
    stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))


def tenant_count():
    out = subprocess.run(PSQL + ["-c",
        "SELECT count(*) FROM proc p JOIN job j ON j.pk_job=p.pk_job "
        "WHERE j.str_name LIKE '%simflten%';"],
        capture_output=True, text=True, timeout=15).stdout.strip()
    return int(out) if out.isdigit() else 0


def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    # Tenants first, one frame per job so placement spreads them. The watcher
    # classifies each host by the tenants actually on it, so imperfect spread
    # only shrinks a class, never breaks an assertion.
    for n in range(1, 5):
        launch(stub, f"sim-test-simfltena-durlong-{n:05d}", 100, 7168, False)
    for n in range(1, 5):
        launch(stub, f"sim-test-simfltenb-durlong-{n:05d}", 300, 2048, True)
    t0 = time.time()
    while time.time() - t0 < 120:
        if tenant_count() >= 8:
            break
        time.sleep(3)
    print(f"tenants running: {tenant_count()}/8; launching the flat flood",
          flush=True)
    launch(stub, "sim-test-simfltx-durshort-00001", 1400, 51200, True,
           frames=FLOOD_FRAMES)
    print(f"SQUEEZE_FLAT: 8 tenants + {FLOOD_FRAMES} flat 14c/50G frames on "
          f"12 small hosts, staying up {DURATION}s.", flush=True)
    while time.time() - t0 < DURATION:
        time.sleep(5)
    print("injector done", flush=True)


if __name__ == "__main__":
    main()
