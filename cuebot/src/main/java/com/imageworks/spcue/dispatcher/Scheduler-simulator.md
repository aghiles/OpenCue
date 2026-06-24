# Scheduler simulator (`scheduler-sim/`)

A DB-backed integration harness for the scheduler. It is not a model of Cuebot,
it **is** Cuebot: a real Cuebot process and a real Postgres, driven over gRPC by a
fake render farm, so every booking goes through the exact production path (the
real `Scheduler`, the real SQL, the real frame-complete handler). That makes it
the integration test unit tests cannot be, and the place to observe behaviour
that only shows up under load: utilization, throughput, co-locality,
reservations, dependency handling, and big-job placement.

## What one run does

A single command brings the whole stack up from nothing and tears it down again:

```
scheduler-sim/simulate.py --mode new --feed 240 --stats 220
```

Each run, from a clean slate:

- initdb's a fresh Postgres cluster, applies the Cuebot schema migrations, and
  seeds the base data (facility, allocation, show, subscription);
- builds Cuebot and starts it, pointing its JVM at a private hosts file so every
  farm hostname resolves to the local fake RQD (no root, no `/etc/hosts` edit);
- registers the farm: **1553 hosts / ~57k cores** across three real host classes
  (jaime 128c, ram 32c, elk 16c), with realistic per-host usable memory;
- starts a **fake RQD** (receives `LaunchFrame`, completes each frame after a
  duration sampled from real-farm distributions) and a continuous host-status
  reporter;
- feeds a workload, streams live stats, and renders utilization + DB-load graphs;
- tears everything down on the next invocation, so **every run starts fresh** and
  re-running is always safe.

Runs are reproducible (seeded RNG) and need no manual setup on a fresh box. The
sim runs as a non-root user (it drops root automatically); every path is a
`SIM_*` env override with a sane default. Default ports: Postgres **5433**,
Cuebot gRPC **8443**, fake RQD **8444**.

## Flags

### Mode & scale

| Flag | Default | What it does |
|---|---|---|
| `--mode new\|old` | `new` | `new` = the E-PVM planner (`Scheduler.java`); `old` = the legacy report-driven dispatcher. |
| `--cuebots N` | `2` | Cuebot instances against one Postgres. All race the advisory lock so one plans per tick, exercising leader election / HA. Use `1` for a single instance. |
| `--hosts j,r,e` | full farm | Shrink the farm to these jaime,ram,elk counts (e.g. `2,3,5`) for legible, watchable debugging. |

### Workload

| Flag | Default | What it does |
|---|---|---|
| `--feed SECONDS` | `0` | Run the paced feeder for SECONDS, holding a sustained backlog. |
| `--feed-target N` | `40000` | Target number of runnable frames the feeder keeps queued. |
| `--jobs N` | `0` | Submit N fixed deterministic jobs at start (instead of a sustained feed). |
| `--dep-tree-depth D` | `3` | Submit each unit of work as an unbalanced dependency tree of max depth D (VFX work is rarely standalone). `1` = independent jobs, no depends. |
| `--seed N` | `9000` | RNG seed, for reproducible runs. |
| `--compress F` | `0.27` | Frame-duration scale (real-minutes → sim-seconds). Higher = longer frames = lower lifecycle rate. |

### Farm realism / placement stress

| Flag | Default | What it does |
|---|---|---|
| `--tags [N]` | off (N=8) | Scatter N random capability tags across the farm; each job requests one, confining it to a ~1/N slice. Stresses placement under tag fragmentation. |
| `--gpu F` | `0` | Make fraction F of the farm GPU-capable and fraction F of layers GPU layers (placed only on GPU hosts). |
| `--mem-heavy [GB]` | off (GB=24) | Flood the farm with small, single-threaded, memory-hungry jobs so RAM binds and cores strand (the realistic sub-100% regime). |
| `--mem-heavy-max-cores N` | `4` | Core cap for the `--mem-heavy` jobs. |
| `--mem-failure-rate F` | `0` | Fraction of frame completions that report an OOM exit, so Cuebot bumps the layer's memory and requeues the frame. |

### Reservations & backfill

| Flag | Default | What it does |
|---|---|---|
| `--reservations` | off | Enable the planner's host reservations for blocked layers (EASY/Maui: time gate + per-class cap). |
| `--reservation-block-seconds S` | `60` | How long a layer must stay blocked before it may reserve. |
| `--reservation-max-fraction F` | `0.5` | Max fraction of a layer's fitting hosts that reservations may hold. |
| `--reservation-max-grantees K` | `8` | Max new reservation grants per tick. |
| `--no-backfill` | backfill on | Disable EASY backfill (reserved hosts freeze idle until they drain). |
| `--strand SECS` | `0` | Starvation test: inject 64-core big jobs every `--strand-interval` for SECS and watch them strand or get rescued by reservations + backfill. Use with `--feed`. |
| `--strand-interval SECS` | `30` | Seconds between big-job injections. |

### Completion reporting

| Flag | Default | What it does |
|---|---|---|
| `--reporter-threads N` | `64` | Fake-RQD completion-report concurrency. A real farm has thousands of RQDs reporting in parallel; a serial reporter caps completion throughput. |
| `--heartbeat-interval S` | `5` (new), `0.1` (old) | Seconds the host-status reporter sleeps between full report rounds. |

### Measurement

| Flag | Default | What it does |
|---|---|---|
| `--metrics SECONDS` | `0` | Run the metrics view (reserved-utilization) for SECONDS. |
| `--stats SECONDS` | `0` | Run the live-stats view for SECONDS: honest live utilization (cores backing running frames), running/waiting counts, frames completing and booking per second, DB load, and steady-state averages. |

Any run with `--feed`, `--strand`, `--stats`, or `--metrics` also auto-records
utilization and DB load and renders graphs at the end (under `/tmp`, override
with `SIM_GRAPH_DIR`).
