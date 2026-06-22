# OpenCue Scheduler, DB-backed farm simulator: STATUS / RESUME NOTES

Branch: `claude/gallant-davinci-mmho1x` (base `origin/SCHEDULER`). Repo: /home/user/OpenCue.

## What this is
A faithful integration test: a REAL cuebot + Postgres, driven over gRPC by a
fake render farm (no DB writes from the driver). Goal: measure the new
`Scheduler` (Scheduler.java) under realistic load, utilization, co-locality,
reservations, big-frame handling.

## Live stack (all as non-root `ubuntu`; postgres/cuebot refuse root)
> `simulate.py` now automates everything in this section: it drops root once
> (re-exec as `SIM_RUN_USER`, default `ubuntu`) and every path is a `SIM_*`
> override with sane defaults, no per-command `sudo`. The notes below record
> the underlying facts / the by-hand equivalents.
- Postgres 16: data /tmp/pgdata (`SIM_PGDATA`), port **5433** (`SIM_PG_PORT`), db `cuebot`, user `cue` (trust).
  start (by hand, as a non-root user): `/usr/lib/postgresql/16/bin/pg_ctl -D /tmp/pgdata -o "-p 5433 -k /tmp/pgrun -c listen_addresses=127.0.0.1" -l /tmp/pg.log start`
- cuebot: gradle bootRun, gRPC **8443**. Needs JDK17 (proxy CA) + gradle 7.6.2.
  Build prep each restart: strip `jcenter()`/`repo.spring.io` from cuebot/settings.gradle+build.gradle, `chown -R ubuntu /home/user/OpenCue`, `rm -rf cuebot/.gradle` if perm errors.
  launch (reservations OFF for bare-core tests), as a non-root user:
  `cd cuebot && env JAVA_TOOL_OPTIONS=-Djdk.net.hosts.file=$PWD/scheduler-sim/sim_hosts CUEBOT_DB_URL=jdbc:postgresql://127.0.0.1:5433/cuebot CUEBOT_DB_USER=cue CUEBOT_DB_PASSWORD= SCHEDULER_ENABLED=true SCHEDULER_INTERVAL_MS=250 SCHEDULER_RESERVATIONS_ENABLED=false ./gradlew bootRun -g /tmp/ghome-$USER --no-daemon -Dorg.gradle.java.home=/tmp/jdk-17.0.2 --console=plain >/tmp/cuebot.log 2>&1`
  IMPORTANT launch pattern: run via Bash tool `run_in_background:true` with NO inner `&` (the `&` double-background gets the JVM SIGKILLed).
- JDK17 at /tmp/jdk-17.0.2 (`SIM_JDK_HOME`; cacerts replaced with managed JDK21's so the proxy TLS works). gradle home /tmp/ghome-<user> (`SIM_GRADLE_HOME`). gradle bin: /opt/gradle-8.14.3 (too new), use the wrapper 7.6.2.

## Driver (Python venv `SIM_VENV_PY`, code in `SIM_FARM` = the scheduler-sim dir)
- `farm_spec.py`    , 246 jaime/128c, 303 ram/32c, 1004 elk/16c; mem 4GB/core.
- `sim_model.py`    , frame cores/mem/duration distribution from the real CSVs; COMPRESS=0.27 real-min→sim-sec.
- `register_hosts.py`,  ReportRqdStartup for all 1553 hosts (RQD->cuebot). One-time.
- `status_pinger.py`, ReportStatus heartbeat every 15s (else cuebot marks hosts DOWN). MUST stay running.
- `fake_rqd.py`     , RQD gRPC server on **8444**; receives LaunchFrame, completes after sampled duration via ReportRunningFrameCompletion. Heap-based (scales). MUST stay running.
- `gen_jobs.py`     , submit 50 jobs (LaunchSpec XML), 2-10 layers, 48-96 frames/layer, cores/mem sampled, `<maxcores>4000</maxcores>` to lift the per-job 100-core default cap.
- `metrics.py [secs]`,  polls DB read-only: util, co-locality, big-frame placement, starvation.
- `kill_all_jobs.py`, kill all sim-show jobs (reset).
- base data seeded once via SQL: /tmp/farm/sim_seed.sql (facility `sim`, alloc `sim.general` default, show `sim`, huge subscription) + dept/services/config from cuebot seed_data.sql.
- All 1553 hostnames -> 127.0.0.1 via a private JVM hosts file (`sim_hosts` + `-Djdk.net.hosts.file`), so cuebot's per-host RQD dial reaches fake_rqd. No root / no /etc/hosts edit (see "Note on host name resolution" below).

Start order: postgres -> cuebot -> status_pinger -> fake_rqd -> register_hosts (if hosts missing) -> gen_jobs -> metrics.

## Bugs found & FIXED (committed/pushed unless noted)
1. `dispatchHost(host,layer)` was an unimplemented stub -> implemented (commit 690f551).
2. `findNextDispatchFrames(layer,host)` transposed params -> 0 frames always (commit c2ac39b).
3. groupByHostSpec fractured per-host because cuebot auto-adds the host name as a tag -> strip it; also bounded dispatch by waitingFrameCount (commit 9e03deb).
4. Hosts age to DOWN without heartbeat -> status_pinger.

## THE BIG FINDINGS (P4, under realistic load)
- **Reservations are catastrophic**: `reconcileReservationsForLayer` reserves ~one host per *pending waiting frame*. With many layers the first ~25 layers reserve the ENTIRE farm, and equal-priority reservations block every other layer (`reservationAllows`). Farm sits reserved-but-idle. Reservations were meant to rescue STARVED layers, not starve everyone. Util 3-8%, big frames never run.
- **Commit doesn't own its frames** (the "really stupid bug"): a CommitTask is just (host,layer). The planner fanned ONE layer across many hosts in a tick; every parallel commit ran `findNextDispatchFrames(layer,host)` and got the SAME next frames -> 74k `frame.int_version` collisions + host overbooking -> "accounting error" kills -> nothing sticks -> whole backlog re-dispatched every tick (thrash). Reservations-ON masked it (limited fan-out -> lock instead); reservations-OFF exposed full thrash (0%).

## FIX (COMMITTED, 47e2b8a)
- `scheduler.reservations_enabled` flag (default true) gating reservationAllows + reconcile; `reservations=N` in the tick log.
- **One commit per layer per tick** (`break;` after `submitCommit` in dispatchGroupWithScoring): a layer is dispatched to a single host per tick (no fan-out), so parallel commits never share a layer/frames.

## RESULTS (validated, 50-job mix, reservations OFF)
- Fix WORKS: version collisions 0 (was ~74k), utilization 0% -> climbs to ~14% and the backlog DRAINS (waiting 14k -> ~150), ~10k frames completed in 70s, big-core (16c) layers run and complete (900 done). Thrash gone.
- Remaining quality issue: **placement skew**: small frames pile onto elk (16c) hosts; jaime(128c)/ram(32c) stay nearly idle (E-PVM stranding-avoidance over-penalises big hosts for small frames). Util is capped partly by this and partly because the 50-job mix drains before saturating.
- "big frames never run" from an earlier run was a METRIC BUG (instantaneous proc sampling), not real, big frames do run.

## KNOWN ENVIRONMENT LIMITS (this box, not scheduler logic)
- Submitting ~150 jobs at once: JobLauncher pool is size 1 / queue 100 -> extra jobs rejected (TaskRejectedException); and Postgres hit `ERROR: unable to allocate additional memory` in `trigger__verify_host_resources()` -> bookings failed and the scheduler stopped ticking. Pace job submission (or tune PG memory) for saturation tests. Restart cuebot+PG to recover.

## NEXT STEPS
- Reservation redesign (currently disable-able via flag): only reserve for layers that are genuinely BLOCKED this tick (dispatched 0 with frames remaining), cap the count to a few hosts, and only block STRICTLY-lower-priority work, not equal priority.
- Placement: revisit E-PVM weights / let small work overflow onto big hosts under load so jaime/ram aren't stranded idle.
- Paced/sustained-load run to measure steady-state utilization and co-locality.

## Useful checks
- ticks: `grep "Scheduler tick: dispatched" /tmp/cuebot.log | tail`
- collisions/kills: `grep -c "updated by another thread" /tmp/cuebot.log` ; `grep -c "accounting error" /tmp/cuebot.log`
- frame states: `sudo -u ubuntu /usr/lib/postgresql/16/bin/psql -h127.0.0.1 -p5433 -Ucue -dcuebot -Atc "SELECT str_state,count(*) FROM frame GROUP BY 1"`

---

# DRAIN RACE: NEW vs OLD scheduler (the moment of truth)

Goal: submit one FIXED backlog (deterministic, seed 9000 -> 30 jobs / 13,857
frames / ~277k core-seconds of work) and time how long each scheduler takes to
drain it to zero. Same farm (246 jaime/128c + 303 ram/32c + 1004 elk/16c =
1,553 hosts, 57,248 cores), same fake-RQD duration model, reservations OFF.
Harness: `scheduler-sim/` + `drain_test.py <label> <njobs> <seed>`.

## Result (FAIR comparison, parallel completion reporter)

| Scheduler | Drain time | Throughput | Peak concurrent | Avg cores busy | Total work |
|-----------|-----------:|-----------:|----------------:|---------------:|-----------:|
| **NEW** (Scheduler.java)        | **87.9 s**  | **158 frm/s** | 1108 | 3143 | 276,259 core-s |
| **OLD** (legacy BookingQueue)   | 217.6 s     | 64 frm/s      |  348 | 1275 | 277,502 core-s |

**NEW wins by 2.48x.** Identical total work (~277k core-seconds, same avg ~3.7
cores/frame), so the difference is pure throughput: NEW keeps the farm ~3x
denser (1108 vs 348 concurrent frames) because it proactively plans across all
hosts every tick. The legacy path only books when a host *reports* (5 s cadence,
few frames/report), so it cannot fill a large farm and leaves most hosts idle.

## CAUTION, a harness artifact nearly inverted the result

First run showed the OPPOSITE: OLD 251.5 s vs NEW 372.2 s. That was wrong, and
finding out why was the important part:

- fake_rqd reported frame completions on a **single thread**, blocking on each
  `ReportRunningFrameCompletion` RPC. That caps completion throughput at
  `1 / ack_latency`.
- Under NEW, cuebot's busy scheduler (planning + the batched commit) raises
  per-completion ack latency (~30 ms vs ~15 ms idle). With a serial reporter the
  whole drain rate collapses to that latency: NEW 13857/372 = 37 frm/s, OLD
  13857/251 = 55 frm/s. The race was measuring **completion-ack latency**, not
  scheduling.
- Real farms have thousands of independent RQDs acking concurrently. Fix:
  report completions from a `ThreadPoolExecutor(64)` (committed in
  `scheduler-sim/fake_rqd.py`) + instrumentation (`ackMs_avg/max`,
  `reporterLagMs`, `cores_launched`, `work_coreSec`). With the realistic
  parallel reporter, the true result emerges (table above): **NEW 87.9 s vs OLD
  217.6 s.**

Lesson for production assessment: validate the *driver* before trusting a
benchmark. A serialized client can make a worse scheduler look better.

## Secondary observations
- **Core over-reservation (grab-idle):** mid-ramp, NEW reserves more cores/frame
  than OLD because threadable frames grab idle cores on near-empty hosts (saw a
  tail of 16-70 core procs). By end-of-run both converge to ~3.7 cores/frame and
  ~277k core-seconds, so it does not change the verdict here, but on a
  capacity-bound workload it would waste cores, and the sim currently *penalises*
  it (duration grows with reserved cores, whereas a truly threadable frame
  should run faster/same). Worth a follow-up: cap grab-idle, and base sim
  duration on the layer's *requested* cores, not runtime-reserved cores.
- **Two regimes, both favour NEW:** the drain race is booking-throughput-bound
  (small backlog, frames finish fast); the earlier sustained-load feed test is
  capacity-bound (NEW climbs to ~74% util, 1552/1553 hosts busy, elk->ram->jaime
  overflow). NEW wins both.

## How to reproduce
```
# NEW: cuebot with SCHEDULER_ENABLED=true SCHEDULER_RESERVATIONS_ENABLED=false
# OLD: cuebot with SCHEDULER_ENABLED=false   (legacy report-driven booking)
# both: fake_rqd.py + status_pinger_fast.py running; wipe sim show between runs
python drain_test.py NEW 30 9000      # -> "[NEW] DRAINED in 87.9s ..."
python drain_test.py OLD 30 9000      # -> "[OLD] DRAINED in 217.6s ..."
# work/latency telemetry is printed by fake_rqd every 5s (ackMs_*, work_coreSec)
```

---

# REALISTIC UTILIZATION TEST: NEW under memory variation + capability tags

Goal: steady-state utilization under production-realistic conditions: lognormal
per-layer memory (σ=0.5), system-reserve usable RAM per host type (jaime 496 GB,
ram 116 GB, elk 56 GB), and capability tags. Farm: 246 jaime/128c + 303 ram/32c +
1004 elk/16c. Reporter threads = 64, compress = 8, 280 s backlog + 220 s window.

> RETRACTION: an earlier version of this section reported NEW ~46%/OLD ~16% and
> attributed a big utilization drop to "tag pool fragmentation." Those numbers
> were invalid on two counts: (1) `feed.py` (the sustained-backlog feeder)
> hard-coded `<tags>general</tags>`, so the fed jobs were never actually tagged:
> the run measured memory variation + OOM only; and (2) the tag model assigned
> N arbitrary pools by random hash, independent of machine size, creating
> impossible combos (a 32-core job pooled with 16-core machines it can't run on).
> Both are fixed (capability tags + feed.py honours them). Clean numbers below.

## Capability tags (the corrected model)

Tags encode machine CLASS: small=elk/16c, med=ram/32c, large=jaime/128c. Each
host advertises every class it can satisfy (jaime {small,med,large}, ram
{small,med}, elk {small}); each layer is tagged with the smallest class whose
machines fit it (cores AND memory). Big layers are confined to big machines;
small work still packs onto big machines. ~98% of layers are "small", ~1.6%
"med", ~0.3% "large". Enforced by cuebot's dispatch query, no scheduler change.

## Results (NEW, identical workload)

| run | util avg | peak | f/s | jaime | ram | elk |
|---|--:|--:|--:|--:|--:|--:|
| no-tags (0.25 s hb, +10% OOM)        | 22.1% | 29.6% | 42 | 8%  | 29% | 76% |
| **capability tags** (0.25 s hb, +OOM)| 31.3% | 44.5% | 54 | 35% | 94% | 54% |
| no-tags, 10 s hb, no OOM             | 25.1% | 31.6% | 70 | 9%  | 31% | 86% |

## Reading the results

**Why ~30%, not the ~69% of the saturation test?** That test used uniform
4 GB/core memory (frames tile perfectly) so a huge backlog saturated elk/ram and
overflowed onto jaime → ~100% peak. Here, two realism factors cap it:

1. **The binding constraint is jaime sitting idle (~8%), a placementScore
   flaw, not correct E-PVM.** jaime holds 31,488 cores = **55% of the farm**.
   `placementScore` charges the **absolute** stranded capacity left after packing
   a host (`W_MEM * leftover_GB`, line 858-878), which scales with host size. For
   a 4-core/6.4 GB frame: elk strands ~30 GB → score ~30; jaime strands ~291 GB →
   score ~291. Same frame shape, but jaime scores ~10× worse purely for being
   bigger, so ~98% small/memory-light work *never* chooses jaime. It would only
   spill there once elk/ram hit 100%, which heavy-tailed memory stranding
   prevents (they top out ~86%/31%). So jobs WAIT while 55% of the farm idles,
   capping farm util near ~45%. Proper E-PVM should normalize stranding to a
   *fraction* of host capacity (relative waste), letting a real backlog fill big
   hosts; "saving" a big host for big work is the job of reservations, not a
   size-biased base score. (Fix is a Scheduler.java change, not yet made.)
2. **Heartbeat and OOM barely matter for utilization.** Dropping the heartbeat
   from 0.25 s → 10 s and removing OOM (row 3) leaves util ~unchanged (25% vs
   22%); it only changes DB cost (the 0.25 s heartbeat floods ~12k tx/s of
   per-frame memory updates). So the low number is memory stranding + placement
   skew, not heartbeat overhead.

**Capability tags *raise* utilization here (+9 pts, +12 f/s).** They force the
~2% med/large layers off the over-subscribed elk tier onto ram/jaime, spreading
load (jaime 8→35%, ram 29→94%). They do not cure the jaime skew, they relocate
the contention (elk→ram). The fix is a best-fit-by-capability nudge in the score
so small work spills to jaime sooner instead of waiting for elk to be 100% full.

**NEW vs OLD** under tags was not re-measured after the fix (the prior OLD
numbers were from the invalid setup). The decisive NEW≫OLD result stands on the
clean DRAIN RACE and SATURATION tests below (NEW 2.5-9× on throughput/util).

## How to reproduce
```bash
# capability tags (--tags is now a flag, not a count)
python simulate.py --mode new --tags --reporter-threads 64 --compress 8 \
    --feed 280 --heartbeat-interval 0.25 --mem-failure-rate 0.1 --stats 220
# no-tags baseline
python simulate.py --mode new --reporter-threads 64 --compress 8 \
    --feed 280 --heartbeat-interval 0.25 --mem-failure-rate 0.1 --stats 220
```

## Zombie proc leak (DIAGNOSED, accepted for now, reaper handles it)

NEW shows a large standing population of zombie procs (`proc.pk_frame IS NULL`):
peak ~1100-1600 across the clean capability/no-tag runs, vs a handful for OLD. A
zombie holds reserved cores+memory but backs no running frame, so it inflates
`live%` and suppresses real throughput. Root cause, fully traced:

1. On frame complete, `DispatchSupportService.stopFrame()` synchronously sets
   `pk_frame = NULL` (`procDao.clearVirtualProcAssignment`). This does **not**
   release the proc's host cores/memory, only `deleteVirtualProc → procDestroyed`
   (`ProcDaoJdbc.deleteVirtualProc`) does.
2. The follow-up that re-books the proc onto the next frame *or* unbooks it
   (`FrameCompleteHandler.handlePostFrameCompleteOperations`, and the
   `DispatchNextFrame` it spawns, which unbooks at `CoreUnitDispatcher:343` when
   no frame is found) is posted **async** to `dispatchQueue`
   (`FrameCompleteHandler.java:183`).
3. `dispatchQueue` is small, 6-8 threads, capacity 2000 (`opencue.properties`).
   When it saturates, `QueueRejectCounter.rejectedExecution()` **silently drops**
   the task (it only increments a counter). The proc is then stranded as a zombie.

**Why NEW ≫ OLD (583 vs 6):** the leak is a nonlinear *drop cliff*, not linear
with throughput. OLD (~13 f/s) never fills the queue → ~0 drops, ~6 transient.
NEW (~51 f/s) plus the heavy per-frame DB cost (~280 tx/frame) saturates the
8-thread/2000-deep queue → once full, *every* completion's cleanup is dropped.

**Why it's bounded (and why we accept it for now):** a zombie's `ts_ping` can
never be refreshed (the memory-usage update is keyed `WHERE pk_frame = ?`,
`ProcDaoJdbc:191`), so it freezes at completion time and the 300s orphan sweep
(`findOrphanedVirtualProcs`, ping-staleness based) reclaims it after 5 minutes.
So it is a *standing population* (≈ leak_rate × 300s), not an unbounded leak, a
small, self-limiting % of the farm. Decision: leave it to the reaper for now.

**Real fix when we revisit:** stop silently dropping proc-lifecycle tasks, on
queue-full, run them on the caller thread (backpressure) instead of discarding
in `QueueRejectCounter`. Optionally add a targeted sweep for `pk_frame = NULL`
procs with no pending cleanup, separate from the 300s ping path.

---

# SATURATION TEST: NEW vs OLD sustained-load utilization

Goal: hold a sustained 40,000-frame backlog for 240 s (paced feeder keeps
waiting ≥ 40k the whole time) and measure steady-state utilization, throughput,
and host coverage. Farm: 246 jaime/128c + 303 ram/32c + 1004 elk/16c = 1553
hosts / 57,248 cores. `--compress 8` → 4-core frame runs ~88 s, 8-core ~240 s,
16-core ~420 s (real-farm shape preserved). Reporter threads = 64 (concurrent
RQD simulation). Harness: `simulate.py --reporter-threads 64 --compress 8 --feed 240 --metrics 220`.

## Result

| Metric | NEW (Scheduler.java) | OLD (legacy BookingQueue) | Ratio |
|--------|---------------------:|-------------------------:|------:|
| **Peak utilization** | **100.0%** | 13.9% | **7.2×** |
| **Avg utilization** (220 s incl. ramp) | **68.8%** | 7.4% | **9.3×** |
| **Throughput** | **31,414 frm / 220 s = 143 frm/s** | 3,720 frm / 220 s = 17 frm/s | **8.4×** |
| **Peak hosts busy** | **1553 / 1553 (100%)** | 370 / 1553 (24%) | **4.2×** |
| **Peak running frames** | 12,445 | 1,777 | 7.0× |
| elk (16c) util at peak | 99.0% | 28.6% |, |
| jaime (128c) util at peak | 99.9% | 8.3% |, |
| ram (32c) util at peak | 99.9% | 8.4% |, |
| Time to 100% util | **~122 s** | never reached |, |

**NEW reaches 100% utilization within 2 minutes and sustains it; OLD plateaus
at ~14% and never climbs further.** The OLD scheduler only books when a host
*reports* (driven by the host-report heartbeat); with 1553 hosts it can drain
the per-report booking path but cannot push work onto idle hosts fast enough to
saturate the farm. NEW's tick-driven planner proactively scans all hosts every
250 ms and batches commits in parallel, the gap widens as the farm grows.

## How to reproduce (one command)
```bash
# NEW, fresh start, 64-thread reporter, 8× compressed frames, 40k sustained backlog
python simulate.py --mode new --reporter-threads 64 --compress 8 --feed 240 --metrics 220

# OLD, same harness, legacy scheduler
python simulate.py --mode old --reporter-threads 64 --compress 8 --feed 240 --metrics 220
```
`simulate.py` always tears down any previous run, resets the DB, and starts
fresh, re-running it is always safe. It also writes a private JVM hosts file
(`sim_hosts`) mapping all farm hostnames → 127.0.0.1 and points cuebot's JVM at
it, so cuebot can dial `<hostname>:8444` to reach `fake_rqd.py`, no root.

## Note on host name resolution (no root)

Cuebot dials `<hostname>:8444` for each host via the RQD gRPC client, so in the
sim every farm name must resolve to 127.0.0.1 (where `fake_rqd.py` listens).
Rather than edit the system `/etc/hosts` (needs root), `simulate.py` writes its
own hosts file `sim_hosts` and starts cuebot with
`JAVA_TOOL_OPTIONS=-Djdk.net.hosts.file=<path>/sim_hosts`. That JDK property
(9+) makes the JVM resolve names from that file alone, a per-process
`/etc/hosts` requiring no privileges. The forked Spring Boot (bootRun) JVM
inherits `JAVA_TOOL_OPTIONS`, so it's in effect before `InetAddress`
initialises; gradle runs `--no-daemon` so a stale daemon can't fork the app JVM
without the property.

Because `jdk.net.hosts.file` becomes the JVM's *only* name source, the file
must also contain `localhost` and the local machine name, `write_sim_hosts_file()`
adds these. To drive the older scripts by hand, launch cuebot the same way:
```bash
python -c "
import sys; sys.path.insert(0,'/tmp/farm'); import farm_spec
print('127.0.0.1 localhost')
for name,_,_ in farm_spec.all_hosts(): print(f'127.0.0.1 {name}')
" > sim_hosts
# start cuebot with: JAVA_TOOL_OPTIONS=-Djdk.net.hosts.file=$PWD/sim_hosts
```
Without name resolution, cuebot logs `UnknownHostException: elk0001` for every
launch attempt, every frame fails with `RqdClientException: failed to launch
frame`, and utilization stays at ~0%.
