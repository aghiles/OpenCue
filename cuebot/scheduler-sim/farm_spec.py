"""Render-farm spec for the OpenCue scheduler simulator.

Host counts/cores are the real farm. Nominal RAM is ~4 GB/core, but the memory
actually bookable by frames is LESS: the OS, RQD, monitoring agents and the
filesystem cache take a cut (see system_reserve in HOST_TYPES). That gap, plus
the heavy-tailed per-layer memory in sim_model, is what makes memory -- not
cores -- the binding constraint on real farms, so utilization never hits 100%
and memory-heavy layers get starved.
"""

GB_KB = 1024 * 1024          # 1 GB expressed in kB (cuebot host mem is kB)
CORES_PER_PROC = 100         # 100 core-points == 1 core

import hashlib
import os

FACILITY = "sim"
ALLOC = "general"
TAG = "general"
SHOW = "sim"

# Connection config shared by every helper script, all overridable via SIM_*
# (matching simulate.py's defaults) so the whole harness agrees. No sudo: the
# scripts run as the same non-root user that started Postgres and cuebot, so
# anyone can run the simulator without root or a specific username.
GRPC = os.environ.get("SIM_CUEBOT_GRPC", "localhost:8443")
PG_BIN = os.environ.get("SIM_PG_BIN", "/usr/lib/postgresql/16/bin")
PG_PORT = os.environ.get("SIM_PG_PORT", "5433")
DB_HOST = os.environ.get("SIM_DB_HOST", "127.0.0.1")
DB_USER = os.environ.get("SIM_DB_USER", "cue")
DB_NAME = os.environ.get("SIM_DB_NAME", "cuebot")


def psql_cmd(tab=False):
    """psql argv for read-only queries. No sudo (runs as the current user);
    host/port/user/db come from SIM_* env so it matches simulate.py. tab=True
    selects a tab field separator (for scripts that split on it)."""
    cmd = [os.path.join(PG_BIN, "psql"), "-h", DB_HOST, "-p", str(PG_PORT),
           "-U", DB_USER, "-d", DB_NAME, "-A", "-t"]
    if tab:
        cmd += ["-F", "\t"]
    return cmd

# Capability tags (the production routing model). SIM_TAGS=1 turns it on.
#
# In production, tags encode machine CLASS, not arbitrary pools: a big box can
# run anything a smaller box can, and a job is routed to the smallest class whose
# machines actually fit it -- you would never tag a 32-core job onto a 16-core
# machine. We model that with size classes tied to the real host types:
#
#   small = elk (16c)   med = ram (32c)   large = jaime (128c)
#
#   * each HOST advertises every class it can satisfy (its own and all smaller),
#     so jaime carries {small,med,large}, ram {small,med}, elk {small};
#   * each LAYER is tagged with the smallest class whose machines fit it (by
#     BOTH cores and usable memory).
#
# Result: big layers are confined to big machines (never tagged onto machines
# that cannot run them), while small work can still pack onto big machines.
# 'general' stays on every host so the allocation lookup resolves. Matching is
# enforced entirely by cuebot's dispatch query -- no scheduler change.
TAGS_ON = os.environ.get("SIM_TAGS", "0") not in ("0", "", "false", "False")

# Size classes in increasing capability, tied to the host types below.
TIERS = ["small", "med", "large"]
_TYPE_TIER = {"elk": 0, "ram": 1, "jaime": 2}


def _stable_frac(key):
    """Deterministic float in [0,1) from a string key."""
    h = hashlib.md5(key.encode()).hexdigest()
    return int(h[:8], 16) / float(0x100000000)


def _tier_caps():
    """{tier_rank: (cores, usable_mem_kb)} ceiling for each class, from HOST_TYPES."""
    caps = {}
    for type_name, _, cores, mem_gb, reserve_gb in HOST_TYPES:
        r = _TYPE_TIER.get(type_name)
        if r is not None:
            caps[r] = (cores, (mem_gb - reserve_gb) * GB_KB)
    return caps


def host_tags(name):
    """Capability tags for a host: 'general' (alloc lookup) plus every size class
    it can satisfy -- its own tier and all smaller ones."""
    tags = [TAG]
    if TAGS_ON:
        r = _TYPE_TIER.get(name.rstrip("0123456789"), 0)
        tags += TIERS[: r + 1]
    return tags


def layer_tier(cores, mem_kb):
    """Smallest size-class tag whose machines fit this layer (cores AND memory),
    or None when tagging is off. Never returns a class too small to run it."""
    if not TAGS_ON:
        return None
    caps = _tier_caps()
    for r in range(len(TIERS)):
        c, m = caps.get(r, (0, 0))
        if cores <= c and mem_kb <= m:
            return TIERS[r]
    return TIERS[-1]   # bigger than any single machine fits -> largest class

# (type name, count, cores, nominal_mem_GB, system_reserve_GB) -- the full farm.
# Usable RAM for frames = nominal - system_reserve. A "128 GB" box does not give
# frames 128 GB: the OS, RQD, monitoring agents and the filesystem cache take a
# cut (bigger boxes run more daemons/cache, so the reserve grows with size).
# Usable lands around 3.5-3.9 GB/core -- tight enough that the heavy-tailed
# per-layer memory (sim_model) strands cores instead of packing perfectly.
HOST_TYPES = [
    ("jaime", 246, 128, 512, 16),
    ("ram",   303,  32, 128, 12),
    ("elk",  1004,  16,  64,  8),
]

# Small-farm override for quick, legible debugging runs. Set SIM_HOST_COUNTS to
# "jaime,ram,elk" counts (e.g. "2,3,5") to keep the per-type core/mem shape but
# shrink the farm so booking dynamics are easy to watch. Every script that
# imports farm_spec then agrees on the same small farm.
_counts = os.environ.get("SIM_HOST_COUNTS")
if _counts:
    _c = [int(x) for x in _counts.split(",")]
    HOST_TYPES = [(t[0], _c[i], t[2], t[3], t[4]) for i, t in enumerate(HOST_TYPES)]


def all_hosts():
    """Yield (hostname, cores, usable_mem_kb) for every host in the farm.

    usable_mem = nominal - system_reserve (see HOST_TYPES): the RAM actually
    bookable by frames after the OS and host agents take their cut.
    """
    for type_name, count, cores, mem_gb, reserve_gb in HOST_TYPES:
        usable_kb = (mem_gb - reserve_gb) * GB_KB
        for i in range(1, count + 1):
            yield (f"{type_name}{i:04d}", cores, usable_kb)


def total_cores():
    return sum(count * cores for _, count, cores, _, _ in HOST_TYPES)


def total_hosts():
    return sum(count for _, count, _, _, _ in HOST_TYPES)


# GPU. SIM_GPU=F: F of the FARM is GPU-capable (drawn only from ram/elk hosts --
# the 32c/16c machines), and F of layers are GPU layers (see sim_model). GPU
# placement is enforced entirely by cuebot's dispatch query (idle_gpus /
# idle_gpu_mem vs the layer's minGpus / minGpuMemory) -- no scheduler change.
# The point is to exercise the scheduler's GPU accounting, not GPU realism.
GPU_FRAC = float(os.environ.get("SIM_GPU", "0"))
GPU_HOST_TYPES = ("ram", "elk")   # only the 32c/16c machines carry GPUs
GPU_CORES_PER_GPU = 4             # a GPU host gets cores/4 GPUs


def host_gpu(name, cores, usable_kb):
    """(num_gpus, total_gpu_mem_kb) for a host; (0, 0) if not GPU-capable.

    Deterministic per host name (stable hash) so register_hosts and rqd_report
    agree -- the heartbeat reports the same GPU spec every round. Only ram/elk
    hosts are eligible; the per-eligible-host probability is scaled so the GPU
    hosts add up to GPU_FRAC of the WHOLE farm. GPU memory is 1:1 with the host's
    usable CPU memory (proportional, per the design)."""
    if GPU_FRAC <= 0:
        return (0, 0)
    type_name = name.rstrip("0123456789")
    if type_name not in GPU_HOST_TYPES:
        return (0, 0)
    eligible = sum(t[1] for t in HOST_TYPES if t[0] in GPU_HOST_TYPES)
    p = min(1.0, GPU_FRAC * total_hosts() / eligible) if eligible else 0.0
    if _stable_frac("gpu:" + name) < p:
        return (max(1, cores // GPU_CORES_PER_GPU), usable_kb)
    return (0, 0)
