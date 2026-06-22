# Scheduler farm simulator

A DB-backed integration harness for the cuebot `Scheduler`. It runs a **real
cuebot + Postgres** and drives them over gRPC with a **fake render farm** — no
database writes from the driver. Hosts register like RQD, jobs are submitted
like a client, and a fake RQD runs/completes frames. Used to measure the
scheduler under realistic load (utilization, co-locality, big-frame handling,
reservations).

This exercises the *real* booking path end to end and has already surfaced
several bugs (see `../src/main/java/com/imageworks/spcue/dispatcher/Scheduler-simulator.md`).

## Pieces
| file | role |
|------|------|
| **`simulate.py`** | **one command: tears down, resets DB, brings the whole stack up fresh, starts a workload** |
| `farm_spec.py` | the farm: 246 jaime/128c, 303 ram/32c, 1004 elk/16c; mem 4 GB/core |
| `sim_model.py` | frame cores/mem/duration distribution (from real-farm CSVs); `SIM_COMPRESS` env scales durations |
| `sim_seed.sql` | one-time base data: facility/alloc/show/subscription |
| `register_hosts.py` | register all hosts via RQD ReportRqdStartup |
| `rqd_report.py [int]` | faithful host **+ running-frame** status heartbeat; refreshes `proc.ts_ping` so the 300s orphan sweep behaves like prod |
| `status_pinger.py` / `status_pinger_fast.py` | older empty-frame heartbeat (kept; superseded by `rqd_report.py`) |
| `fake_rqd.py [threads]` | fake RQD gRPC server on :8444; runs+completes frames. `threads`=completion-report concurrency (1=serial, 64=concurrent RQDs) |
| `gen_jobs.py` | submit a realistic job mix via LaunchSpec |
| `feed.py [dur] [target]` | paced feeder: hold a sustained backlog of ~`target` waiting frames |
| `drain_test.py <label> <njobs> <seed>` | submit a fixed backlog, time to drain (NEW vs OLD races) |
| `metrics.py [secs]` | poll DB read-only: util, co-locality, big-frame progress |
| `stats.py [secs]` | per-host-type util + honest live-util + **zombie/orphan proc counter** |
| `kill_all_jobs.py` | kill all sim jobs (reset) |

## One command (simulate.py)
After the one-time prereqs below, this is all you need — it **always starts
fresh** (kills any prior run, ensures Postgres, wipes the sim show, resets every
host to idle, restarts cuebot in the chosen mode, brings up fake RQD + the
status reporter, then starts the workload):
```
# sustained-load utilization, NEW scheduler, realistic concurrency:
python simulate.py --mode new --reporter-threads 64 --compress 8 --feed 240 --metrics 220

# same on the legacy/old booking path (the control):
python simulate.py --mode old --reporter-threads 64 --compress 8 --feed 240 --metrics 220

# fixed-backlog drain, NEW:
python simulate.py --mode new --jobs 30 --seed 9000
```
Key flags: `--mode new|old` (scheduler.enabled), `--reporter-threads N`
(completion concurrency), `--compress F` (frame length; keep
`hosts/avg_duration` under cuebot's ~120 lifecycle/s ceiling so the farm fills),
`--feed S` / `--feed-target T`, `--jobs N` / `--seed S`, `--metrics S`,
`--heartbeat-interval S`, `--tags N` (production tag pools: N tags + an untagged
pool; ~3/4 of hosts and jobs each take one tag, ~1/4 stay untagged and run
anywhere — placement is enforced by cuebot's dispatch query, no scheduler
change). Run `python metrics.py 120` against a live run anytime.

> Note: `simulate.py` selects scheduler behaviour only via documented
> properties; it never modifies cuebot code.

## Prereqs (one-time)
**Automatic:** run `./setup.sh` from this directory. It creates `venv/`, installs
the gRPC deps, generates `opencue_proto/` from `../../proto/src`, and checks for a
usable JDK 17. Then run the sim with `./venv/bin/python simulate.py ...`.
(`simulate.py` also self-generates `opencue_proto/` on first run if it's missing,
so the proto step self-heals.)

The manual equivalent, if you'd rather do it by hand:
1. **Postgres** reachable; apply cuebot Flyway migrations to a `cuebot` DB, then
   the base seed: dept/services/config from `../src/main/resources/conf/ddl/postgres/seed_data.sql`
   plus `sim_seed.sql`.
2. **Python deps**: `python -m venv venv && venv/bin/pip install grpcio grpcio-tools protobuf`.
3. **Proto stubs**: compile the OpenCue protos into `opencue_proto/` (scripts add
   it to `sys.path`):
   `venv/bin/python -m grpc_tools.protoc -I../../proto/src --python_out=opencue_proto --grpc_python_out=opencue_proto ../../proto/src/*.proto && touch opencue_proto/__init__.py`
4. **Host name resolution (no root needed)**: cuebot dials `<hostname>:8444`
   for every host via the RQD gRPC client, so every farm name must resolve to
   127.0.0.1 (where `fake_rqd.py` listens). **`simulate.py` handles this with
   zero privileges**: it writes a private hosts file (`sim_hosts`) and points
   cuebot's JVM at it with `-Djdk.net.hosts.file=...` (a per-process name source,
   JDK 9+). No `/etc/hosts` edit, no `sudo`. If you drive the older scripts by
   hand, launch cuebot the same way:
   ```bash
   python -c "
   import sys; sys.path.insert(0,'.')
   import farm_spec
   print('127.0.0.1 localhost')
   for name,_,_ in farm_spec.all_hosts(): print(f'127.0.0.1 {name}')
   " > sim_hosts
   # then start cuebot with: JAVA_TOOL_OPTIONS=-Djdk.net.hosts.file=$PWD/sim_hosts
   ```
   Without name resolution, cuebot logs `UnknownHostException: elk0001` for every
   launch attempt and utilization stays at ~0% (all frames fail with
   `RqdClientException: failed to launch frame`). Note `jdk.net.hosts.file` makes
   that file the JVM's **only** name source, so it must also contain `localhost`
   (and the local machine name) — `simulate.py` adds these automatically.

## Paths & environment (all overridable)
`simulate.py` has no hardcoded layout — every path has a default plus a `SIM_*`
env override, so it runs from a plain checkout. Defaults derive from the
script's own location:

| var | default | what it is |
|-----|---------|------------|
| `SIM_FARM` | the dir of `simulate.py` | where the helper scripts + `opencue_proto/` live |
| `SIM_CUEBOT_DIR` | parent of `SIM_FARM` | cuebot project root (has `gradlew`) |
| `SIM_VENV_PY` | the interpreter running `simulate.py` | Python used for the helper scripts |
| `SIM_JDK_HOME` | `/tmp/jdk-17.0.2` | JDK for gradle; if the dir is absent, the ambient `JAVA_HOME` is used |
| `SIM_GRADLE_HOME` | `/tmp/ghome-<user>` | gradle user home (`-g`) |
| `SIM_PG_BIN` | `/usr/lib/postgresql/16/bin` | dir with `psql`/`pg_ctl` |
| `SIM_PGDATA` | `/tmp/pgdata` | Postgres data dir |
| `SIM_PG_PORT` | `5433` | Postgres port |
| `SIM_CUEBOT_LOG` | `/tmp/cuebot.log` | cuebot stdout/stderr log |
| `SIM_RUN_USER` | `$SUDO_USER`, else the checkout owner | non-root user to drop to **iff** invoked as root |

So from a checkout the common case is simply:
```
# scheduler-sim lives in cuebot/, so SIM_CUEBOT_DIR defaults correctly;
# run with the venv that has the deps and FARM/VENV_PY resolve themselves:
path/to/venv/bin/python simulate.py --mode new --hosts 1,1,2 --jobs 3 --stats 30
```
If invoked as **root**, `simulate.py` re-execs itself as `SIM_RUN_USER`
(postgres/cuebot refuse root), forwarding all `SIM_*` vars across the drop. If
invoked by a normal user it runs as them with no `sudo` anywhere.

## Run cuebot with the scheduler on
Set `scheduler.enabled=true` (and optionally `scheduler.interval_ms`,
`scheduler.reservations_enabled`). Point cuebot at the same Postgres.

## Drive it
```
venv/bin/python status_pinger.py &     # heartbeat (keep running)
venv/bin/python fake_rqd.py &          # fake RQD (keep running)
venv/bin/python register_hosts.py      # once, registers 1553 hosts
venv/bin/python gen_jobs.py            # submit the job mix
venv/bin/python metrics.py 70          # observe
```

Notes: postgres and cuebot must run as a non-root user (both refuse root).
`simulate.py` handles this for you: if invoked as root it re-execs itself as a
non-root user (`SIM_RUN_USER`, defaulting to whoever `sudo`'d in, else the owner
of the checkout) and runs the whole stack there; if invoked by a normal user it
just runs as them, with no `sudo` anywhere. So the sim works for any user out of
the box, with no account name hardcoded.
See `../src/main/java/com/imageworks/spcue/dispatcher/Scheduler-simulator.md`
for full environment/run notes and the findings log.
