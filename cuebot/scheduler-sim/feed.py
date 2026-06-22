"""Paced job feeder: keep a sustained backlog so the farm stays oversubscribed.

Submits waves of jobs (unique names) to hold ~TARGET waiting frames, backing
off on launch-queue rejection. Realistic mix from sim_model. Run alongside
stats.py to measure steady-state utilization.

usage: feed.py [duration_s] [target_waiting]
"""
import os, sys, time, random, subprocess
import grpc
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import job_pb2, job_pb2_grpc
import sim_model
import farm_spec as spec

CUEBOT = spec.GRPC
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300
TARGET = int(sys.argv[2]) if len(sys.argv) > 2 else 40000
WAVE = 6                      # jobs per submit burst (launch queue is pool=1/q=100)
LAYERS_MIN, LAYERS_MAX = 2, 10
FRAMES_MIN, FRAMES_MAX = 48, 96
PSQL = spec.psql_cmd()

SPEC_HEAD = ('<?xml version="1.0"?>\n'
  '<!DOCTYPE spec SYSTEM "http://localhost:8080/spcue/dtd/cjsl-1.15.dtd">\n'
  '<spec>\n  <facility>sim</facility>\n  <show>sim</show>\n  <shot>test</shot>\n'
  '  <user>sim</user>\n  <uid>9860</uid>\n')

def waiting():
    try:
        out = subprocess.run(PSQL+["-c","SELECT count(*) FROM frame WHERE str_state='WAITING' AND int_depend_count=0;"],
                             capture_output=True, text=True, timeout=10).stdout.strip()
        return int(out)
    except Exception:
        return -1

def make_job(name, seq, rng):
    n = rng.randint(LAYERS_MIN, LAYERS_MAX)
    layers = []
    for li in range(n):
        nf = rng.randint(FRAMES_MIN, FRAMES_MAX)
        if sim_model.is_gpu_layer():
            # GPU layer (honours --gpu): few cores + 1 GPU + gpu_memory; cpu mem
            # = half the gpu mem. Only GPU hosts can run it (cuebot enforces).
            cores, gpu_mem_kb, cpu_mem_kb = sim_model.gpu_layer_kb()
            mem_kb = cpu_mem_kb
            lname = f"l{li}_g{cores}"
            gpu_xml = (f'<gpus>1</gpus>'
                       f'<gpu_memory>{int(round(gpu_mem_kb/1024))}mb</gpu_memory>')
        else:
            cores = sim_model.sample_layer_cores()
            mem_kb = sim_model.mem_kb_for(cores)
            lname = f"l{li}_c{cores}"
            gpu_xml = ""
        mem_mb = int(round(mem_kb/1024))
        # Capability tag: smallest host class that fits this layer (else 'general').
        tag = spec.layer_tier(cores, mem_kb) or spec.TAG
        layers.append(f'      <layer name="{lname}" type="Render">'
            f'<cmd>/bin/true</cmd><range>1-{nf}</range><chunk>1</chunk>'
            f'<cores>{cores*sim_model.CORE_POINTS}</cores><threadable>{sim_model.THREADABLE}</threadable><memory>{mem_mb}mb</memory>'
            f'{gpu_xml}<tags>{tag}</tags><services><service>shell</service></services></layer>')
    return (f'  <job name="{name}"><paused>false</paused><maxcores>8000</maxcores>\n'
            '    <layers>\n' + "\n".join(layers) + "\n    </layers>\n  </job>\n")

def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = job_pb2_grpc.JobInterfaceStub(chan)
    t0 = time.time(); submitted = 0; seq = 0
    while time.time()-t0 < DURATION:
        w = waiting()
        if 0 <= w < TARGET:
            for _ in range(WAVE):
                seq += 1
                job_xml = SPEC_HEAD + make_job(
                    f"sim-test-sim_f{int(t0)%100000}_{seq:05d}", seq,
                    random.Random(seq)) + "</spec>\n"
                try:
                    stub.LaunchSpec(job_pb2.JobLaunchSpecRequest(spec=job_xml))
                    submitted += 1
                except grpc.RpcError:
                    time.sleep(2.0)   # launch queue full -> back off
                    break
        print(f"t={time.time()-t0:5.0f}s waiting={w} submitted={submitted}", flush=True)
        time.sleep(2.0)
    print(f"feeder done: submitted {submitted} jobs", flush=True)

if __name__ == "__main__":
    main()
