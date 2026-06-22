"""Faithful host+frame status heartbeat (replaces the empty-frame pinger).

Real RQD periodically reports, for each host, the host's resource state AND the
RunningFrameInfo of every frame currently executing on it. cuebot uses those
running-frame entries to refresh proc.ts_ping (UPDATE_PROC_MEMORY_USAGE) and to
verify procs. Our previous pinger sent empty frame lists, so proc.ts_ping was
never refreshed and the 300s orphan sweep (ts_ping based) would eventually
mis-flag live procs -- a sim artifact, not a cuebot bug.

This reporter reconstructs each host's running frames from the proc table (a DB
read, which the harness already does for measurement) and reports them, so
ts_ping stays fresh for live procs exactly like production. Idle hosts still get
an empty-frame heartbeat to stay UP. Memory values are reported well under the
layer limits so the handler never memory-kills a frame.

usage: rqd_report.py [interval_s]   (default 0.1 = report continuously)

The interval is the SLEEP between full report rounds. In OLD/legacy mode cuebot
only books a host when that host reports, so a slow interval starves the
report-driven booker and the farm never fills; report near-continuously (0.1)
to mimic a real farm of thousands of always-reporting RQDs.
"""
import os
import sys
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor

import grpc

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import report_pb2, report_pb2_grpc
import host_pb2
import farm_spec as spec

CUEBOT = spec.GRPC
INTERVAL = float(sys.argv[1]) if len(sys.argv) > 1 else 0.1
# Reports are sent CONCURRENTLY across this many threads. A serial round of 1553
# synchronous ReportStatus RPCs collapses to cuebot's per-report latency (and
# grows as cuebot gets busy), so a round took seconds -- far slower than the
# interval and starving OLD-mode booking. Concurrency lets a full round finish
# sub-second regardless of per-report latency (a real farm reports in parallel).
REPORT_THREADS = int(sys.argv[2]) if len(sys.argv) > 2 else 64
SHOW = "10000000-0000-0000-0000-000000000003"
PSQL = spec.psql_cmd(tab=True)
RSS_KB = 512 * 1024  # report 512MB rss -- safely under every layer's mem limit

HOSTS = list(spec.all_hosts())                    # (name, cores, mem_kb)
HOST_INFO = {n: (c, m) for n, c, m in HOSTS}


def running_by_host():
    """{host_name: [(proc_id, frame_id, job_id, job_name, frame_name, layer_id,
                     cores, mem_kb, gpus, gpu_mem_kb), ...]}"""
    sql = (
        "SELECT h.str_name, p.pk_proc, p.pk_frame, p.pk_job, j.str_name, "
        "f.str_name, p.pk_layer, p.int_cores_reserved, p.int_mem_reserved, "
        "p.int_gpus_reserved, p.int_gpu_mem_reserved "
        "FROM proc p "
        "JOIN host h ON h.pk_host=p.pk_host "
        "JOIN job j ON j.pk_job=p.pk_job "
        "JOIN frame f ON f.pk_frame=p.pk_frame "
        "WHERE p.pk_frame IS NOT NULL;")
    out = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True, timeout=30).stdout
    by_host = {}
    for line in out.strip().split("\n"):
        if not line:
            continue
        c = line.split("\t")
        if len(c) < 11:
            continue
        by_host.setdefault(c[0], []).append(
            (c[1], c[2], c[3], c[4], c[5], c[6], int(c[7]), int(c[8]),
             int(c[9]), int(c[10])))
    return by_host


def render_host(name, cores, mem_kb, free_mem_kb, gpus, gpu_mem_kb, free_gpu_mem_kb):
    return report_pb2.RenderHost(
        name=name, facility=spec.FACILITY,
        num_procs=cores, cores_per_proc=spec.CORES_PER_PROC,
        total_mem=mem_kb, free_mem=free_mem_kb,
        total_swap=8 * spec.GB_KB, free_swap=8 * spec.GB_KB,
        total_mcp=100 * spec.GB_KB, free_mcp=100 * spec.GB_KB,
        num_gpus=gpus, total_gpu_mem=gpu_mem_kb, free_gpu_mem=free_gpu_mem_kb,
        load=0, boot_time=1, nimby_enabled=False,
        state=host_pb2.UP, tags=spec.host_tags(name))


def frame_info(rec, now):
    (proc_id, frame_id, job_id, job_name, frame_name, layer_id,
     cores, mem_kb, gpus, gpu_mem_kb) = rec
    return report_pb2.RunningFrameInfo(
        resource_id=proc_id, job_id=job_id, job_name=job_name,
        frame_id=frame_id, frame_name=frame_name, layer_id=layer_id,
        num_cores=cores, num_gpus=gpus, start_time=now - 1,
        rss=RSS_KB, max_rss=RSS_KB, vsize=RSS_KB, max_vsize=RSS_KB,
        used_gpu_memory=0, max_used_gpu_memory=0, llu_time=now)


def _send_one(stub, name, cores, mem_kb, frames, now):
    cp = cores * spec.CORES_PER_PROC
    gpus, gpu_mem_kb = spec.host_gpu(name, cores, mem_kb)
    booked_cp = sum(r[6] for r in frames)
    # Report REAL available memory: usable RAM minus what running frames have
    # reserved. (Real RQD reports actual free RAM; reporting full RAM made the
    # host always look empty.) Booking uses int_mem_idle, not this value, so
    # this only fixes host_stat/monitoring; it stays >=0 and we leave swap
    # untouched, so cuebot's host-OOM balancer is not spuriously triggered.
    booked_mem = sum(r[7] for r in frames)
    free_mem = max(0, mem_kb - booked_mem)
    # Same idea for GPU memory: report usable minus what GPU frames reserved.
    booked_gpu_mem = sum(r[9] for r in frames)
    free_gpu_mem = max(0, gpu_mem_kb - booked_gpu_mem)
    report = report_pb2.HostReport(
        host=render_host(name, cores, mem_kb, free_mem,
                         gpus, gpu_mem_kb, free_gpu_mem),
        frames=[frame_info(r, now) for r in frames],
        core_info=report_pb2.CoreDetail(
            total_cores=cp, idle_cores=max(0, cp - booked_cp),
            locked_cores=0, booked_cores=booked_cp))
    try:
        stub.ReportStatus(report_pb2.RqdReportStatusRequest(host_report=report))
        return 0
    except grpc.RpcError:
        return 1


def ping_round(stub, pool):
    now = int(time.time())
    running = running_by_host()
    # Fire all host reports CONCURRENTLY (see REPORT_THREADS). gRPC channels are
    # thread-safe for concurrent unary calls, so a busy cuebot no longer
    # serializes the round behind per-report latency.
    futs = [pool.submit(_send_one, stub, name, cores, mem_kb,
                        running.get(name, []), now)
            for name, cores, mem_kb in HOSTS]
    failed = sum(f.result() for f in futs)
    return sum(len(v) for v in running.values()), failed


def main():
    chan = grpc.insecure_channel(CUEBOT)
    grpc.channel_ready_future(chan).result(timeout=15)
    stub = report_pb2_grpc.RqdReportInterfaceStub(chan)
    rounds = 0
    with ThreadPoolExecutor(max_workers=REPORT_THREADS) as pool:
        while True:
            t0 = time.time()
            nframes, failed = ping_round(stub, pool)
            rounds += 1
            print(f"report round {rounds}: {len(HOSTS)} hosts, {nframes} running "
                  f"frames in {time.time()-t0:.2f}s "
                  f"({REPORT_THREADS} threads){f' ({failed} failed)' if failed else ''}",
                  flush=True)
            time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
