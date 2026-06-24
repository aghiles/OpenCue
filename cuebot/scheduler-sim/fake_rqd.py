"""Fake RQD: one gRPC server impersonating every farm host.

cuebot dials each host at <hostname>:8444 (all -> 127.0.0.1 via /etc/hosts), so
one server receives every LaunchFrame. Each launched frame is scheduled for
completion after a duration sampled from the real distribution (by core count),
then reported complete to cuebot. A single timer thread drains a min-heap of
due completions, so we scale to thousands of concurrent frames without a thread
per frame.

Memory-failure simulation: set SIM_MEM_FAILURE_RATE=0.1 (env or argv[2]) to
inject a 10% chance that each frame completion reports exit_status=33
(Dispatcher.EXIT_STATUS_MEMORY_FAILURE). Cuebot then:
  1. increaseLayerMemoryRequirement (bumps int_mem_min by ~2 GB)
  2. disables the memory optimizer for that layer
  3. unbooks the proc and requeues the frame to WAITING
The frame retries on the next scheduling tick; memory failures bypass cuebot's
max-retry guard so a frame always gets another chance.
"""
import os
import sys
import time
import heapq
import random
import threading
from concurrent import futures

import grpc

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "opencue_proto"))
sys.path.insert(0, _HERE)
import rqd_pb2, rqd_pb2_grpc
import report_pb2, report_pb2_grpc
import host_pb2
import sim_model
import farm_spec as spec

CUEBOT = spec.GRPC
RQD_PORT = 8444

_report = report_pb2_grpc.RqdReportInterfaceStub(grpc.insecure_channel(CUEBOT))

_heap = []            # (due_time, seq, RunningFrameInfo)
_heap_lock = threading.Lock()
_seq = 0
_stats = {"launched": 0, "completed": 0, "mem_failed": 0, "failed": 0,
          # work + latency instrumentation
          "core_points": 0,        # sum of reserved core-points launched (100==1 core)
          "work_cs": 0.0,          # sum of (cores * sim_duration) -> core-seconds of work
          "ack_ms_sum": 0.0,       # sum of completion-RPC latency (cuebot-side processing)
          "ack_ms_max": 0.0,
          "lag_ms_sum": 0.0}       # sum of (report_start - due_time): reporter backlog

# exit_status=33 == Dispatcher.EXIT_STATUS_MEMORY_FAILURE
_EXIT_MEM_FAILURE = 33

_DUMMY_HOST = report_pb2.RenderHost(
    name="fake-rqd", nimby_locked=False, free_mem=64 * 1024 * 1024, state=host_pb2.UP)

# Completion-reporting concurrency. 1 (default) = serial, which matches the
# original harness baseline. A larger pool models many independent RQDs acking
# concurrently (useful for drain-throughput tests) but raises contention between
# the central planner and the legacy keep-proc rebooking. Set via argv[1] or the
# RQD_REPORTER_THREADS env var so a run is explicit and reproducible.
import os
_REPORTER_THREADS = int(
    sys.argv[1] if len(sys.argv) > 1 else os.environ.get("RQD_REPORTER_THREADS", "1"))
_report_pool = futures.ThreadPoolExecutor(max_workers=max(1, _REPORTER_THREADS))

# Memory failure injection rate (0.0 = disabled, 0.1 = 10% of frame completions
# report exit_status=33 so cuebot increases the layer memory and requeues the frame).
_MEM_FAILURE_RATE = float(
    sys.argv[2] if len(sys.argv) > 2 else os.environ.get("SIM_MEM_FAILURE_RATE", "0"))


def _send_completion(frame, due_time):
    mem_fail = _MEM_FAILURE_RATE > 0 and random.random() < _MEM_FAILURE_RATE
    exit_status = _EXIT_MEM_FAILURE if mem_fail else 0
    report = report_pb2.FrameCompleteReport(
        host=_DUMMY_HOST, frame=frame, exit_status=exit_status, exit_signal=0, run_time=1)
    t0 = time.time()
    try:
        _report.ReportRunningFrameCompletion(
            report_pb2.RqdReportRunningFrameCompletionRequest(
                frame_complete_report=report))
        ack = (time.time() - t0) * 1000.0
        with _heap_lock:
            if mem_fail:
                _stats["mem_failed"] += 1
            else:
                _stats["completed"] += 1
            _stats["ack_ms_sum"] += ack
            if ack > _stats["ack_ms_max"]:
                _stats["ack_ms_max"] = ack
            _stats["lag_ms_sum"] += max(0.0, (t0 - due_time) * 1000.0)
    except grpc.RpcError:
        with _heap_lock:
            _stats["failed"] += 1


def _completion_loop():
    while True:
        now = time.time()
        due = []
        with _heap_lock:
            while _heap and _heap[0][0] <= now:
                item = heapq.heappop(_heap)
                due.append((item[2], item[0]))
        for frame, due_time in due:
            _report_pool.submit(_send_completion, frame, due_time)
        time.sleep(0.02)


class RqdServicer(rqd_pb2_grpc.RqdInterfaceServicer):
    def LaunchFrame(self, request, context):
        global _seq
        rf = request.run_frame
        dur = sim_model.duration_seconds(rf.num_cores)
        frame = report_pb2.RunningFrameInfo(
            resource_id=rf.resource_id, job_id=rf.job_id, job_name=rf.job_name,
            frame_id=rf.frame_id, frame_name=rf.frame_name, layer_id=rf.layer_id,
            num_cores=rf.num_cores, start_time=int(time.time()),
            max_rss=512 * 1024, rss=512 * 1024)
        with _heap_lock:
            _stats["launched"] += 1
            _stats["core_points"] += rf.num_cores
            _stats["work_cs"] += (max(1, rf.num_cores // 100)) * dur
            heapq.heappush(_heap, (time.time() + dur, _seq, frame))
            _seq += 1
        return rqd_pb2.RqdStaticLaunchFrameResponse()

    def KillRunningFrame(self, request, context):
        return rqd_pb2.RqdStaticKillRunningFrameResponse()

    def GetRunningFrameStatus(self, request, context):
        return rqd_pb2.RqdStaticGetRunningFrameStatusResponse()


def _stats_loop():
    while True:
        time.sleep(5)
        with _heap_lock:
            pending = len(_heap)
            c = _stats["completed"] or 1
            ack_avg = _stats["ack_ms_sum"] / c
            lag_avg = _stats["lag_ms_sum"] / c
            cs = _stats["work_cs"]; cp = _stats["core_points"]
        mem_fail_str = (f" memFailed={_stats['mem_failed']}"
                        f"({100*_stats['mem_failed']/max(1,_stats['launched']):.0f}%)"
                        if _MEM_FAILURE_RATE > 0 else "")
        print(f"  [rqd] launched={_stats['launched']} completed={_stats['completed']}"
              f"{mem_fail_str} pending={pending} failed={_stats['failed']} "
              f"cores_launched={cp//100} work_coreSec={cs:.0f} "
              f"ackMs_avg={ack_avg:.1f} ackMs_max={_stats['ack_ms_max']:.0f} "
              f"reporterLagMs_avg={lag_avg:.1f}", flush=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
    rqd_pb2_grpc.add_RqdInterfaceServicer_to_server(RqdServicer(), server)
    server.add_insecure_port(f"[::]:{RQD_PORT}")
    server.start()
    threading.Thread(target=_completion_loop, daemon=True).start()
    threading.Thread(target=_stats_loop, daemon=True).start()
    mem_str = (f", mem_failure_rate={_MEM_FAILURE_RATE:.0%}"
               if _MEM_FAILURE_RATE > 0 else "")
    print(f"fake RQD listening on :{RQD_PORT}, reporting to {CUEBOT} "
          f"(reporter threads={_REPORTER_THREADS}{mem_str})", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
