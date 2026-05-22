# Scheduler (Planner)

A single-threaded, whole-farm scheduler for OpenCue, gated behind
`scheduler.enabled` (default **off**). It is an alternative to the legacy
per-host dispatcher. When enabled it owns dispatch and the legacy
`BookingQueue` path is suppressed.

This document explains what it does, how it works, the concurrency model
that keeps it correct, how to configure it, and what is planned next.

---

## 1. Background: dispatcher vs scheduler

The legacy path is a **dispatcher**. When a host reports in, it runs
`findDispatchJobs(host)` (a heavy multi-table join) and books the first
frame that fits, in priority order, one host at a time. It never sees more
than one host at once, so it cannot reason across the farm: it cannot hold
a big machine open for a wide job that is queued, or steer a small frame
onto a small machine instead of wasting a big one.

In practice that gap has been filled outside Cuebot, by an operator-tuned
script (`cue-layer-man`) that rewrites each layer's core request and tags
ahead of time so the naive dispatcher behaves. That works, but it pushes
the real scheduling decision onto a hand-maintained, per-show rulebook.

A **scheduler** makes those decisions itself. It takes a snapshot of the
whole farm each cycle, scores every candidate placement by how much
capacity it would strand, holds reservations for work that would otherwise
starve, and books accordingly. That is what this component does.

---

## 2. Architecture: the tick loop

The scheduler runs a periodic tick (`runTick` → `doTick`). Each tick:

1. **Snapshot** — read all bookable hosts in one query (`readBookableHosts`,
   `SELECT_BOOKABLE_HOSTS`). Hosts that are UP, OPEN, and have at least the
   minimum bookable cores.
2. **Group** — bucket hosts by spec key `(alloc, normalized_tags, os,
   has_gpu)` (`groupByHostSpec`). On a homogeneous farm this is a handful of
   groups, which is what collapses the per-host query storm into a few
   queries per tick.
3. **For each group:**
   1. **Candidate query** — one query per group
      (`readLayerCandidatesForGroup`, `SELECT_CANDIDATES_FOR_GROUP`) for the
      dispatchable layers that match the group, ordered by priority + age.
   2. **Dispatch + reconcile** (`dispatchGroupWithScoring`) — for each
      candidate in priority order, score every fitting host, pick the lowest
      score, submit a commit, and decrement the in-memory snapshot. Then
      reconcile the layer's reservations.
4. **Await commits** (`awaitCommitsDrain`) — barrier: wait for this tick's
   commits to land so the next snapshot reflects them.
5. **Sweep** — drop reservations whose layer no longer appears in any
   candidate set.

The planner thread does steps 1–3 single-threaded. The commits in 3.ii run
in parallel on a worker pool while the planner keeps going; step 4 is just a
barrier.

---

## 3. Components

### 3.1 Placement score (multi-resource E-PVM)

`placementScore(host, layer)` — lower is better. It scores a host by how
much capacity is left **stranded** after packing it with as many frames of
this layer as the dispatcher would admit. The remaining surplus across each
resource (cores, memory, GPUs, GPU memory) is weighted and summed:

```
score = W_CORES   * stranded_cores
      + W_MEM     * stranded_mem_gb
      + W_GPUS    * stranded_gpus
      + W_GPU_MEM * stranded_gpu_mem_gb
```

Default weights: `W_CORES=1`, `W_MEM=1`, `W_GPUS=4`, `W_GPU_MEM=1`. Cores
and memory contribute equally; GPUs are weighted higher to discourage
placing non-GPU work on GPU-rich hosts and stranding expensive capacity.

This is a simple E-PVM / opportunity-cost score (Amir, Awerbuch, Barak,
Borgström & Keren 2000; extended to multi-resource in Verma et al., Borg,
EuroSys 2015). It is the same "match the job's resource ratio to the
machine's so nothing is wasted" logic that an operator-tuned routing
rulebook approximates by hand, computed here from live state.

### 3.2 Reservations

A layer that cannot fit anywhere claims hosts in proportion to its pending
unfittable frames, so wide work is not starved by a stream of small frames.

- `reservationAllows(host, layer)` — a reservation lets a layer through if
  there is no reservation, the reservation belongs to that layer, or the
  existing reservation is strictly lower priority (in which case the
  higher-priority layer overrides it on successful dispatch).
- `reconcileReservationsForLayer` — each tick, bring the layer's reservation
  count to its pending frame count: drop excess, claim more via
  `pickReservationTarget`.
- `pickReservationTarget` — choose the host most likely to free up soonest
  for the layer (fewest running procs), among hosts that can fit it when
  fully idle and are not reserved at equal-or-higher priority.

The reservation map persists across ticks. The invariant: a host's
reservation belongs to the highest-priority layer that has claimed it. The
end-of-tick sweep drops reservations for layers that left the dispatchable
set.

This is the mechanism that retires the human-driven "save this machine for
the big job" practice.

### 3.3 Commit pool

The planner never writes bookings itself. For each placement it submits a
`CommitTask(hostId, layerId, priority)` to a priority-ordered queue consumed
by a pool of workers (`scheduler.commit_pool_size`, default 8). Each worker
fetches the host fresh and calls `dispatcher.dispatchHost(host, layer)` —
the same per-pairing booking path the legacy dispatcher uses.

This gives I/O parallelism (DB transactions + RQD gRPC) without putting the
*decisions* on multiple threads. Decisions stay single-writer; only the
commits fan out.

### 3.4 Leader election

Only one Cuebot may plan at a time. `runTick` takes a Postgres advisory lock
(`pg_try_advisory_lock`); a Cuebot that does not hold it returns
immediately. The lock is released at the end of each tick and is released
automatically by Postgres if the leader's session drops, so failover is
automatic. A new leader starts with an empty reservation map and rebuilds
the same set within a tick or two, so there is no persistent reservation
state to migrate.

---

## 4. Concurrency model

The planner reasons over an in-memory snapshot while commits and external
events change the database in the background. This is safe by design.

**Single-booker invariant.** Three guards ensure nothing competes to
*consume* capacity behind the planner's back:

1. `tickInFlight` compare-and-set — one Cuebot never overlaps its own ticks.
2. Leader advisory lock — only one Cuebot plans across the deployment.
3. `scheduler.enabled` suppresses the legacy `BookingQueue` enqueue in
   `HostReportHandler`, so the old dispatcher is not booking.

So the only things that can change host state during a tick are:

- **The planner's own commits** — already accounted for, because the planner
  decrements its in-memory snapshot for each decision as it makes it.
- **External frame completions** — these only *free* cores, i.e. the
  snapshot is conservative (it under-counts free capacity). Safe.

**Snapshot drift is a quality issue, not a correctness one.** The commit
path treats the database as the source of truth, not the snapshot:
`dispatchHost` re-reads the host and books against real current state, with
two hard guards underneath —

- the atomic host update
  `UPDATE host SET int_cores_idle = int_cores_idle - ? WHERE ... >= ?`,
  which makes physical over-booking impossible, and
- the `frame.int_version` optimistic lock, which rejects any overlapping
  frame grab.

If a commit books fewer frames than the planner estimated (host had less
room than the snapshot, or a sibling commit got there first), the planner
merely over-decremented its in-memory copy and leaves that host slightly
under-packed for the rest of the tick. The next tick's fresh snapshot
corrects it. Failures bias toward **under-booking** (waste a little capacity
for one tick), never over-booking.

**The drain barrier** bounds drift to a single tick: after
`awaitCommitsDrain`, the database fully reflects this tick's decisions, so
the next snapshot re-grounds on reality. Because commits run in parallel
during planning, the barrier is usually catching only stragglers, not adding
latency. (Tradeoff: the tick rate is gated by the slowest commit — if one
commit hits a slow transaction or sluggish RQD, the next tick waits at the
barrier. See Future Work.)

---

## 5. Configuration

| Property | Default | Meaning |
|---|---|---|
| `scheduler.enabled` | `false` | Master switch. When on, the planner owns dispatch and the legacy BookingQueue is suppressed. |
| `scheduler.commit_pool_size` | `8` | Number of parallel commit workers. |
| `scheduler.layer_candidates_per_group_max` | `2000` | Cap on candidate layers fetched per group per tick. |
| `dispatcher.job_frame_dispatch_max` | `8` | Max frames of one job booked onto a host per commit. |
| `dispatcher.host_frame_dispatch_max` | `12` | Max frames booked onto a host per commit call. |

No schema changes. The only new SQL is the host-snapshot query and the
per-group candidate query, both against existing tables and indexes.

**Rollback** is a single flag: set `scheduler.enabled=false` and the legacy
dispatcher resumes.

---

## 6. Failure modes

- **Commit collision** (`frame.int_version` / resource guard): the frame is
  not booked, stays waiting, picked up next tick. Logged at debug.
- **Leader loss** (session drop): the advisory lock releases automatically;
  another Cuebot becomes leader on its next tick and rebuilds reservations.
- **Slow commit**: blocks the drain barrier and delays the next tick. See
  Future Work for the timeout escape hatch.
- **Empty snapshot** (no bookable hosts): tick is a no-op; reservations are
  left intact.

---

## 7. Future work

### 7.1 Batched commits

Today each frame is booked in its own transaction (`startFrameAndProc` is
`@Transactional`, called per frame inside `dispatchHost`). Booking N frames
of a layer onto a host is N transactions. Because the single-booker
invariant makes collisions rare, we can batch:

- Wrap one `dispatchHost` call in a single transaction (N frames, one
  BEGIN/COMMIT).
- Multi-row proc INSERT (no optimistic check, trivially batchable).
- Summed host decrement (one `UPDATE host` instead of N).
- Batched frame-state update via a VALUES join keyed on `(pk_frame,
  int_version)`, comparing affected-row count to detect partial collisions
  and retrying just those.

Target: batch **per host per tick**, keeping cross-host parallelism on the
worker pool. Expected to cut commit-side DB time substantially (transaction
overhead dominates per-frame cost). Does not affect the RQD gRPC path, which
is per frame and already parallel. Watch row-lock duration vs the
frame-complete handler.

### 7.2 Memory-aware sizing (retire the OOM workaround)

The operator rulebook over-pins cores largely to claim enough memory so
frames survive RSS growth (Cuebot otherwise bumps layer memory by 1 GB and
retries on OOM — a slow per-frame climb). Cuebot already records max-RSS.
The planner could read the layer's learned memory reservation and size
placement on that directly, removing the need for the core-pinning
workaround entirely.

### 7.3 Tags as hard constraints only

With the planner in place, tags should mean hard constraints (license, GPU
model, OS), not host-class routing (e.g. "big/mid/small"). Routing is what
the placement score computes. Retiring the scheduling-purpose tags collapses
the spec groups into larger, more flexible pools and gives the score more
room to optimize.

### 7.4 Slow-commit timeout

A timeout-and-carry-on option for the drain barrier: let a straggler commit
finish but do not block the next tick past N ms. Prevents one pathological
commit from stalling planning.

### 7.5 DB load under concurrency (ceiling)

The scheduler greatly reduces query load versus the legacy per-host path. A
fuller model of DB behaviour under concurrent load (queries slowing each
other, contention with GUI/API traffic) would help predict headroom at
larger scale. Not required for correctness.

---

## 8. Testing

An offline simulator that A/B tests the legacy dispatcher, the Rust
dispatcher, and this planner against a production-shaped workload lives on
the `sim` branch under `benchmarks/sim_cpp/`. It models the workload service
mix, the `cue-layer-man` pre-pass, KSM co-location, and simulated DB time.
Use it for algorithmic experiments; use a real small-facility trial for
production validation.
