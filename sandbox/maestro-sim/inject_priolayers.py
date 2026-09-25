"""PRIOLAYERS workload: job priority against layer count, on one managed show.

Two jobs of one show contend for the same cores. HI is priority 80 with one
layer; LO is priority 30 with LO_LAYERS layers. Every layer is the same
shape: one-core, non-threadable, light memory, long frames (durlong) so the
split holds still long enough to read. The legacy dispatcher walks jobs by
priority, so HI runs first and LO gets what is left. Maestro's slot draw is
per layer and each layer is capped at a quarter of every host, so LO's layers
outdraw HI's one and HI leaves the tick once it holds its quarter everywhere:
layer count beats priority, and LO ends up with several times HI's cores.

Both jobs are submitted paused and released with one UPDATE once every job
exists and both hold runnable frames, so arrival order decides nothing. The
show runs in Maestro's managed mode (SIM_MAESTRO_ENABLED=managed), as the
production rollout does.

usage: inject_priolayers.py [duration_s]
"""
import os, subprocess, sys, time
import grpc
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import job_pb2, job_pb2_grpc
import sim_model
import farm_spec as spec

CUEBOT = spec.GRPC
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300
PRI_HI = int(os.environ.get("SIM_PRIOLAYERS_HI", "80"))
PRI_LO = int(os.environ.get("SIM_PRIOLAYERS_LO", "30"))
LO_LAYERS = int(os.environ.get("SIM_PRIOLAYERS_LO_LAYERS", "8"))
FRAMES = int(os.environ.get("SIM_PRIOLAYERS_FRAMES", "4000"))   # per job
FRAMES_MIN = 500                  # runnable frames per class before the release
TOKEN = "simpriolayers"
CLASSES = {"hijob": (PRI_HI, 1), "lojob": (PRI_LO, LO_LAYERS)}
PSQL = spec.psql_cmd()

SPEC_HEAD = ('<?xml version="1.0"?>\n'
             '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
             f'<spec>\n  <facility>sim</facility>\n  <show>{spec.SHOW}</show>\n'
             '  <shot>test</shot>\n  <user>sim</user>\n  <uid>9860</uid>\n')


def job(name, pri, layers, frames):
    per = max(1, frames // layers)
    body = "\n".join(
        f'      <layer name="lyr{i}" type="Render"><cmd>/bin/true</cmd>'
        f'<range>1-{per}</range><chunk>1</chunk>'
        f'<cores>{sim_model.CORE_POINTS}</cores>'
        f'<threadable>0</threadable><memory>512mb</memory>'
        f'<tags>{spec.TAG}</tags>'
        f'<services><service>shell</service></services></layer>'
        for i in range(layers))
    return (f'  <job name="{name}"><paused>true</paused>'
            f'<priority>{pri}</priority><maxcores>80000</maxcores>\n'
            f'    <layers>\n{body}\n    </layers>\n  </job>\n')


def sql(q):
    return subprocess.run(PSQL + ["-c", q], capture_output=True, text=True,
                          timeout=20).stdout.strip()


def jobs():
    out = sql(f"SELECT count(*) FROM job WHERE str_name LIKE '%{TOKEN}%';")
    return int(out) if out.isdigit() else 0


def waiting(cls):
    out = sql("SELECT count(*) FROM frame f JOIN job j ON j.pk_job=f.pk_job"
              f" WHERE j.str_name LIKE '%{TOKEN}%' AND j.str_name LIKE '%{cls}%'"
              " AND f.str_state='WAITING';")
    return int(out) if out.isdigit() else 0


def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    for cls, (pri, layers) in CLASSES.items():
        xml = SPEC_HEAD + job(f"sim-test-{TOKEN}-{cls}-durlong", pri, layers, FRAMES) + "</spec>\n"
        stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=xml))
        print(f"PRIOLAYERS: {cls} pri {pri}, {layers} layer(s), {FRAMES} frames, paused",
              flush=True)

    t0 = time.time()
    deadline = t0 + 90
    while time.time() < deadline and (jobs() < len(CLASSES)
                                      or any(waiting(c) < FRAMES_MIN for c in CLASSES)):
        time.sleep(2)
    sql(f"UPDATE job SET b_paused=false WHERE str_name LIKE '%{TOKEN}%' AND b_paused=true;")
    print(f"PRIOLAYERS: released both jobs together after {time.time() - t0:.0f}s",
          flush=True)

    while time.time() - t0 < DURATION:
        time.sleep(5)
    print("injector done", flush=True)


if __name__ == "__main__":
    main()
