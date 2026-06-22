"""One command to run the OpenCue scheduler simulation from a clean slate.

It ALWAYS starts fresh: it tears down any previous run (cuebot + fake RQD +
pinger + feeders), ensures Postgres is up, wipes the sim show's frames/jobs and
resets every host to idle, then brings the whole stack back up and starts a
workload. Long-running processes (cuebot, fake_rqd, pinger) are left running so
you can observe them; re-running simulate.py cleans them up again.

It does NOT modify cuebot. Scheduler behaviour is selected only via the
documented properties (scheduler.enabled / scheduler.reservations_enabled).

Examples:
  # fresh NEW scheduler, hold a 40k-frame backlog for 240s, print metrics
  python simulate.py --mode new --feed 240 --metrics 220

  # fresh OLD (legacy) scheduler, same workload
  python simulate.py --mode old --feed 240 --metrics 220

  # fresh NEW, submit a fixed 30-job backlog and just leave it running
  python simulate.py --mode new --jobs 30

Run metrics any time against the live run:  python metrics.py 120
"""
import argparse
import getpass
import os
import signal
import socket
import subprocess
import sys
import time

# The user the sim runs as. Postgres and cuebot both REFUSE to run as root, so
# if invoked as root we re-exec the whole script as a non-root user (see
# reexec_as_nonroot_if_needed). When invoked by a normal user this never fires
# and the sim simply runs as them: no sudo, no privilege drop, nothing keyed to
# a specific account. The drop target (root case ONLY) resolves, in order, to
# SIM_RUN_USER, else the user who sudo'd in (SUDO_USER), else the owner of this
# checkout, so there is never a baked-in username like "ubuntu".
def _default_run_user():
    import pwd
    return (os.environ.get("SIM_RUN_USER")
            or os.environ.get("SUDO_USER")
            or pwd.getpwuid(os.stat(os.path.abspath(__file__)).st_uid).pw_name)


RUN_USER = _default_run_user()

# ---------------------------------------------------------------- paths
# Every path below has a sensible default and an env override, so the sim runs
# from a plain checkout with no edits. Defaults are derived from this script's
# own location: FARM is the dir holding the helper scripts (this file's dir),
# CUEBOT_DIR is its parent (scheduler-sim lives inside cuebot/). Override any of
# them with the SIM_* env vars when your layout differs (e.g. a detached copy).
SIM_DIR = os.path.dirname(os.path.abspath(__file__))
FARM = os.environ.get("SIM_FARM", SIM_DIR)
CUEBOT_DIR = os.environ.get("SIM_CUEBOT_DIR", os.path.dirname(SIM_DIR))
# Python that runs the helper scripts. Default: the same interpreter running
# simulate.py — so `path/to/venv/bin/python simulate.py` just works. Override
# with SIM_VENV_PY to point at a different venv.
VENV_PY = os.environ.get("SIM_VENV_PY", sys.executable)
# JDK home for gradle. Empty/missing => let gradle use the ambient JAVA_HOME.
JDK17 = os.environ.get("SIM_JDK_HOME", "/tmp/jdk-17.0.2")
# Gradle user home, per-user so two users on the same box don't clash.
GHOME = os.environ.get("SIM_GRADLE_HOME", f"/tmp/ghome-{getpass.getuser()}")
PGBIN = os.environ.get("SIM_PG_BIN", "/usr/lib/postgresql/16/bin")
PGDATA = os.environ.get("SIM_PGDATA", "/tmp/pgdata")
PG_PORT = int(os.environ.get("SIM_PG_PORT", "5433"))
GRPC_PORT = 8443
SHOW = "10000000-0000-0000-0000-000000000003"
TOTAL_HOSTS = 1553   # full farm; overridden by --hosts (small-farm debug mode)
CUEBOT_LOG = os.environ.get("SIM_CUEBOT_LOG", "/tmp/cuebot.log")
# Per-JVM hosts file. cuebot dials each RQD at <hostname>:8444, so every farm
# name must resolve to 127.0.0.1 (where fake_rqd listens). Rather than touch the
# system /etc/hosts (needs root), we point cuebot's JVM at its OWN hosts file via
# -Djdk.net.hosts.file=... (JDK 9+). No root, no system-wide changes.
SIM_HOSTS_FILE = f"{FARM}/sim_hosts"
RQD_LOG = f"{FARM}/rqd.log"
PINGER_LOG = f"{FARM}/pinger.log"
FEED_LOG = f"{FARM}/feed.log"

PSQL = [f"{PGBIN}/psql", "-h", "127.0.0.1",
        "-p", str(PG_PORT), "-U", "cue", "-d", "cuebot", "-A", "-t"]


def log(msg):
    print(f"[simulate {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def read_text(path):
    """Read a log file tolerantly. gRPC/JVM can emit non-UTF-8 bytes, so decode
    with errors ignored rather than crashing the orchestrator on a stray byte."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def ensure_proto_stubs():
    """Auto-generate opencue_proto/ if it's missing, so a fresh checkout works
    without a separate build step. Stubs come from the repo's proto sources
    (CUEBOT_DIR/../proto/src). Idempotent: skips if already present."""
    proto_pkg = os.path.join(FARM, "opencue_proto")
    if os.path.exists(os.path.join(proto_pkg, "report_pb2.py")):
        return
    proto_src = os.path.join(os.path.dirname(CUEBOT_DIR), "proto", "src")
    if not os.path.isdir(proto_src):
        log(f"WARNING: opencue_proto/ missing and proto sources not at {proto_src}; "
            f"run setup.sh or set SIM_FARM to a dir that has opencue_proto/")
        return
    log(f"opencue_proto/ missing — generating from {proto_src} ...")
    os.makedirs(proto_pkg, exist_ok=True)
    import glob
    r = sh([VENV_PY, "-m", "grpc_tools.protoc", f"-I{proto_src}",
            f"--python_out={proto_pkg}", f"--grpc_python_out={proto_pkg}"]
           + sorted(glob.glob(os.path.join(proto_src, "*.proto"))))
    if r.returncode != 0:
        log(f"  proto generation failed: {r.stderr.strip()[-300:]}")
        sys.exit(1)
    open(os.path.join(proto_pkg, "__init__.py"), "a").close()
    log(f"  generated {len(glob.glob(os.path.join(proto_pkg, '*_pb2.py')))} proto modules")


def ensure_buildable():
    """Fail early with a clear message if CUEBOT_DIR has no gradlew (the usual
    cause is running a detached copy of the scripts outside the repo)."""
    if not os.path.exists(os.path.join(CUEBOT_DIR, "gradlew")):
        log(f"ERROR: no gradlew in CUEBOT_DIR={CUEBOT_DIR}. The sim must reach "
            f"cuebot's gradlew. Run from cuebot/scheduler-sim/, or set "
            f"SIM_CUEBOT_DIR to the cuebot dir that contains gradlew.")
        sys.exit(1)


def _interp_has_grpc(py):
    """True if interpreter `py` can import grpc (i.e. the sim's helpers --
    feed.py, fake_rqd.py, gen_jobs.py -- will run under it)."""
    try:
        return subprocess.run([py, "-c", "import grpc"],
                              capture_output=True, timeout=30).returncode == 0
    except Exception:
        return False


def ensure_grpc_interpreter():
    """Make `python3 simulate.py` work no matter which interpreter launched it.

    Every helper the sim spawns (feed.py, fake_rqd.py, gen_jobs.py, ...) is run
    with this script's own interpreter and imports grpc. If we were started by a
    python WITHOUT grpcio, those helpers die silently at import -- no fake RQD,
    no feeder, and the run reports 0% utilization with no obvious cause. Rather
    than make the user remember to activate a venv, we self-heal:

      1. If our interpreter already has grpc, do nothing.
      2. Else find the sim's own venv (SIM_DIR/venv, or $SIM_VENV_PY). Build it
         via setup.sh if it's missing or also lacks grpc (idempotent).
      3. Re-exec this script under that venv interpreter so every child inherits
         it. SIM_GRPC_REEXECED guards against a re-exec loop.

    Runs AFTER the root drop so the venv is created/owned by the run user.
    """
    if _interp_has_grpc(sys.executable):
        return
    if os.environ.get("SIM_GRPC_REEXECED") == "1":
        sys.exit("ERROR: venv interpreter still cannot import grpc after "
                 "setup.sh; check scheduler-sim/setup.sh output.")
    venv_py = os.environ.get("SIM_VENV_PY") or os.path.join(SIM_DIR, "venv", "bin", "python")
    if not (os.path.exists(venv_py) and _interp_has_grpc(venv_py)):
        setup = os.path.join(SIM_DIR, "setup.sh")
        log(f"this interpreter ({sys.executable}) has no grpc; "
            f"building the sim venv via {setup} ...")
        r = subprocess.run(["bash", setup])
        if r.returncode != 0:
            sys.exit(f"ERROR: setup.sh failed (rc={r.returncode}); cannot build "
                     f"a venv with grpcio. Run scheduler-sim/setup.sh by hand.")
        venv_py = os.path.join(SIM_DIR, "venv", "bin", "python")
    if not (os.path.exists(venv_py) and _interp_has_grpc(venv_py)):
        sys.exit(f"ERROR: no usable venv with grpcio at {venv_py}. "
                 f"Run scheduler-sim/setup.sh by hand.")
    log(f"re-exec under sim venv interpreter {venv_py}")
    os.environ["SIM_GRPC_REEXECED"] = "1"
    os.execv(venv_py, [venv_py, os.path.abspath(__file__)] + sys.argv[1:])


def reexec_as_nonroot_if_needed():
    """If running as root, re-exec the whole script as a non-root user.

    Postgres and cuebot refuse to run as root, so the sim can't run as root
    directly. Instead of sprinkling `sudo -u <user>` through every command, we
    drop privileges once, here, and run everything else plainly as that user.
    When the sim is invoked by a normal user this is a no-op — nothing uses
    sudo at all, so it works for any user out of the box.
    """
    if os.geteuid() != 0:
        return  # already non-root: run everything directly, no sudo
    if os.environ.get("SIM_REEXECED") == "1":
        sys.exit("re-exec as non-root did not drop privileges (SIM_RUN_USER "
                 f"resolved to '{RUN_USER}', still root); set SIM_RUN_USER to a "
                 "non-root account and retry")
    log(f"running as root; postgres/cuebot refuse root, re-exec as '{RUN_USER}'")
    # sudo strips the environment, so forward the sim's own config across the
    # drop: every SIM_* override plus a few pass-throughs. -H sets HOME to
    # RUN_USER's home; SIM_REEXECED guards against a re-exec loop.
    forward = {k: v for k, v in os.environ.items()
               if k.startswith("SIM_") or k in ("JAVA_TOOL_OPTIONS", "PATH")}
    forward["SIM_REEXECED"] = "1"
    env_args = [f"{k}={v}" for k, v in forward.items()]
    os.execvp("sudo", ["sudo", "-H", "-u", RUN_USER, "env"] + env_args
              + [sys.executable, os.path.abspath(__file__)] + sys.argv[1:])


def psql(sql, timeout=60):
    return sh(PSQL + ["-c", sql], timeout=timeout)


def db_stats(sample_s=5):
    """Print a short Postgres health summary: transaction/tuple RATES (sampled
    over sample_s), cache hit ratio, connection states, and the busiest tables.
    Called by default at the end of every run so each sim reports DB load."""
    cols = ("xact_commit, xact_rollback, blks_hit, blks_read, tup_returned, "
            "tup_fetched, tup_inserted, tup_updated, tup_deleted")
    snap = f"SELECT {cols} FROM pg_stat_database WHERE datname='cuebot'"
    try:
        a = [int(x) for x in psql(snap).stdout.strip().split("|")]
        t0 = time.time()
        time.sleep(sample_s)
        b = [int(x) for x in psql(snap).stdout.strip().split("|")]
        dt = time.time() - t0
    except (ValueError, IndexError):
        log("DB stats: unavailable")
        return
    names = ["commits", "rollbacks", "blks_hit", "blks_read", "tup_returned",
             "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted"]
    log(f"==== DB STATS (rates over {dt:.0f}s) ====")
    for n, va, vb in zip(names, a, b):
        log(f"  {n:13s} {(vb - va) / dt:10.0f}/s")
    hit, rd = b[2], b[3]
    if hit + rd:
        log(f"  cache_hit      {100 * hit / (hit + rd):9.2f}%")
    conns = psql("SELECT state, count(*) FROM pg_stat_activity "
                 "WHERE datname='cuebot' GROUP BY state").stdout.strip()
    log("  connections:   " + "; ".join(conns.split("\n")))
    tbls = psql("SELECT relname, n_tup_ins, n_tup_upd, n_tup_del "
                "FROM pg_stat_user_tables ORDER BY "
                "(n_tup_ins+n_tup_upd+n_tup_del) DESC LIMIT 5").stdout.strip()
    log("  busiest tables (ins/upd/del):")
    for row in tbls.split("\n"):
        if row:
            p = row.split("|")
            log(f"    {p[0]:16s} {p[1]:>10}/{p[2]:>10}/{p[3]:>10}")


def port_open(port):
    s = socket.socket()
    s.settimeout(1)
    try:
        return s.connect_ex(("127.0.0.1", port)) == 0
    finally:
        s.close()


def pkill(pattern):
    """Kill processes whose command line matches pattern (best effort)."""
    out = sh(["pgrep", "-f", pattern]).stdout.split()
    me = str(os.getpid())
    killed = 0
    for pid in out:
        if pid == me:
            continue
        try:
            os.kill(int(pid), signal.SIGKILL)
            killed += 1
        except (ProcessLookupError, ValueError, PermissionError):
            pass
    return killed


# ---------------------------------------------------------------- teardown
def teardown():
    log("tearing down any previous run ...")
    # Kill workload + helpers first, then cuebot last.
    for pat in ["feed.py", "inject_big.py", "strand_watch.py", "gen_jobs.py",
                "drain_test.py", "metrics.py", "stats.py", "status_pinger",
                "rqd_report.py", "fake_rqd.py"]:
        pkill(pat)
    pkill("CuebotApplication")
    pkill("gradlew bootRun")
    pkill("build/libs/cuebot.jar")   # extra cuebots launched from the jar
    # Wait for the gRPC port to actually free up.
    for _ in range(30):
        if not port_open(GRPC_PORT):
            break
        time.sleep(1)
    log("  previous run stopped")


# ---------------------------------------------------------------- postgres
# Schema + base seed live in the cuebot tree; sim_seed.sql lives beside us.
DDL_DIR = os.path.join(CUEBOT_DIR, "src", "main", "resources", "conf", "ddl", "postgres")


def _maint_psql(sql, db="postgres", timeout=120):
    """psql as the cluster superuser (the OS user initdb created) on a
    maintenance DB -- used to create the cue role / cuebot DB before they exist."""
    return sh([f"{PGBIN}/psql", "-h", "127.0.0.1", "-p", str(PG_PORT),
               "-U", getpass.getuser(), "-d", db, "-Atqc", sql], timeout=timeout)


def _psql_file(path, db="cuebot", user="cue", timeout=600):
    return sh([f"{PGBIN}/psql", "-h", "127.0.0.1", "-p", str(PG_PORT),
               "-U", user, "-d", db, "-v", "ON_ERROR_STOP=1", "-q", "-f", path],
              timeout=timeout)


def ensure_database():
    """Make a FRESH cluster usable with zero manual steps: create the cue role
    and cuebot DB, apply the cuebot schema (Flyway migrations in version order --
    cuebot bundles Flyway as a TEST-only dep and never migrates at runtime, so the
    schema must be pre-applied), then the base seed (seed_data.sql) and the sim
    entities (sim_seed.sql). Every step is guarded on a cheap existence check, so
    on an already-initialised cluster this is just a few SELECTs."""
    if _maint_psql("SELECT 1 FROM pg_roles WHERE rolname='cue'").stdout.strip() != "1":
        log("  creating role 'cue' ...")
        _maint_psql("CREATE ROLE cue LOGIN SUPERUSER")
    if _maint_psql("SELECT 1 FROM pg_database WHERE datname='cuebot'").stdout.strip() != "1":
        log("  creating database 'cuebot' ...")
        _maint_psql("CREATE DATABASE cuebot OWNER cue")
    import glob
    migs = sorted(glob.glob(os.path.join(DDL_DIR, "migrations", "V*.sql")),
                  key=lambda p: int(os.path.basename(p).split("__", 1)[0][1:]))
    have_show = bool(
        _maint_psql("SELECT to_regclass('public.show')", db="cuebot").stdout.strip())
    have_tracking = bool(_maint_psql(
        "SELECT to_regclass('public.sim_schema_migrations')", db="cuebot").stdout.strip())
    if have_show and not have_tracking:
        # Legacy cluster from before migration tracking: we cannot tell which
        # migrations it already has, so rebuild the DB from scratch to guarantee
        # it matches the current migration set. The data is wiped every run
        # anyway; this triggers once, on the first run after this change or after
        # the migration set grows under a cluster that predates tracking.
        log("  schema predates migration tracking; rebuilding cuebot DB ...")
        _maint_psql("DROP DATABASE cuebot WITH (FORCE)")
        _maint_psql("CREATE DATABASE cuebot OWNER cue")
    # Track applied migrations so a cluster that survives across runs still picks
    # up migrations added upstream since it was created (only the data, not the
    # schema, was previously refreshed each run).
    _maint_psql("CREATE TABLE IF NOT EXISTS sim_schema_migrations "
                "(filename text PRIMARY KEY, applied_at timestamptz DEFAULT now())",
                db="cuebot")
    applied = set(_maint_psql(
        "SELECT filename FROM sim_schema_migrations", db="cuebot").stdout.split())
    pending = [m for m in migs if os.path.basename(m) not in applied]
    if pending:
        log(f"  applying {len(pending)} cuebot schema migrations "
            f"({len(applied)} already applied) ...")
        for m in pending:
            r = _psql_file(m)
            if r.returncode != 0:
                sys.exit(f"schema migration {os.path.basename(m)} failed:\n"
                         f"{(r.stderr or r.stdout)[-800:]}")
            _maint_psql("INSERT INTO sim_schema_migrations(filename) VALUES ('"
                        + os.path.basename(m) + "')", db="cuebot")
    if _maint_psql(f"SELECT 1 FROM show WHERE pk_show='{SHOW}'",
                   db="cuebot").stdout.strip() != "1":
        log("  seeding base data (seed_data.sql + sim_seed.sql) ...")
        # Migrations V35/V38 already seed two task_lock rows that seed_data.sql
        # also inserts; clear the table first so seed_data.sql owns the full set
        # and does not collide on those PKs.
        _maint_psql("DELETE FROM task_lock", db="cuebot")
        for f in (os.path.join(DDL_DIR, "seed_data.sql"),
                  os.path.join(FARM, "sim_seed.sql")):
            r = _psql_file(f)
            if r.returncode != 0:
                sys.exit(f"seed {os.path.basename(f)} failed:\n"
                         f"{(r.stderr or r.stdout)[-800:]}")


def ensure_postgres():
    if not port_open(PG_PORT):
        # Fresh box: no cluster yet. initdb one with trust auth on loopback
        # (postgres refuses root; the sim already runs as a non-root user).
        if not os.path.exists(os.path.join(PGDATA, "PG_VERSION")):
            log(f"initialising a fresh Postgres cluster at {PGDATA} ...")
            r = sh([f"{PGBIN}/initdb", "-D", PGDATA, "-A", "trust",
                    "-E", "UTF8", "--no-sync"], timeout=180)
            if r.returncode != 0:
                sys.exit(f"initdb failed:\n{(r.stderr or r.stdout)[-800:]}")
        log("starting postgres ...")
        os.makedirs("/tmp/pgrun", exist_ok=True)
        sh([f"{PGBIN}/pg_ctl", "-D", PGDATA,
            "-o", (f"-p {PG_PORT} -k /tmp/pgrun -c listen_addresses=127.0.0.1"
                   " -c synchronous_commit=off"),
            "-l", "/tmp/pg.log", "start"], timeout=60)
        for _ in range(30):
            if port_open(PG_PORT):
                log("  postgres up")
                break
            time.sleep(1)
        else:
            sys.exit("postgres failed to start; see /tmp/pg.log")
    else:
        log(f"postgres already up on :{PG_PORT}")
    # Idempotently ensure role/db/schema/seed exist (no-op once set up).
    ensure_database()


# ---------------------------------------------------------------- reset
RESET_SQL = (
    "DELETE FROM proc;"
    f" DELETE FROM frame f USING job j WHERE f.pk_job=j.pk_job AND j.pk_show='{SHOW}';"
    f" DELETE FROM layer l USING job j WHERE l.pk_job=j.pk_job AND j.pk_show='{SHOW}';"
    f" DELETE FROM job WHERE pk_show='{SHOW}';"
    " UPDATE host SET int_cores_idle=int_cores, int_mem_idle=int_mem,"
    " int_gpus_idle=int_gpus, int_gpu_mem_idle=int_gpu_mem;"
    " UPDATE subscription SET int_cores=0, int_gpus=0;")


# When shrinking the farm we must physically remove the extra hosts (locking
# doesn't hold: HostReportHandler.changeLockState auto-unlocks on report).
WIPE_HOSTS_SQL = (
    "DELETE FROM proc;"
    " DELETE FROM comments WHERE pk_host IS NOT NULL;"
    " DELETE FROM host_local; DELETE FROM job_local; DELETE FROM deed;"
    " DELETE FROM host_stat; DELETE FROM host;")


def reset_db(wipe_hosts=True):
    """Wipe the sim show AND all hosts. Cuebot must be DOWN (no contention).

    Always removes ALL hosts so they are re-created fresh by register_hosts every
    run: nothing is preserved between runs (the project rule), and host hardware
    set only at creation -- GPUs (int_gpus / int_gpu_mem), tags, cores -- always
    reflects the current farm_spec. (cuebot only refreshes GPU from a report when
    the host is fully idle, which never holds once NEW fills it, so re-creating is
    the reliable path.)
    """
    log("resetting DB to a clean slate (sim show + all hosts wiped) ...")
    r = psql(RESET_SQL)
    if "ERROR" in (r.stdout + r.stderr):
        log("  WARN reset hit: " + (r.stdout + r.stderr).strip().splitlines()[-1])
    if wipe_hosts:
        rh = psql(WIPE_HOSTS_SQL)
        if "ERROR" in (rh.stdout + rh.stderr):
            log("  WARN host wipe hit: " + (rh.stdout + rh.stderr).strip().splitlines()[-1])
    chk = psql(
        f"SELECT (SELECT count(*) FROM proc), "
        f"(SELECT count(*) FROM frame f JOIN job j ON j.pk_job=f.pk_job "
        f"WHERE j.pk_show='{SHOW}'), (SELECT count(*) FROM host);")
    procs, frames, hosts = chk.stdout.strip().split("|")
    log(f"  procs={procs} simFrames={frames} hosts={hosts}")
    if int(procs) or int(frames):
        sys.exit("reset failed (procs/frames remain) -- is cuebot still up?")
    return int(hosts)


# ---------------------------------------------------------------- cuebot
def ensure_cuebot_built():
    """Warm the gradle build (wrapper dist + deps + compile) with NORMAL name
    resolution. start_cuebot runs bootRun under -Djdk.net.hosts.file=sim_hosts so
    the cuebot APP resolves farm hostnames to fake_rqd -- but that file becomes the
    JVM's ONLY name source, so a COLD build under it cannot reach
    services.gradle.org / Maven Central and dies with UnknownHostException.
    Building here first (no hosts file) makes the later bootRun build-cache-only,
    leaving the hosts file to govern only the app's RQD dials. A no-op once warm."""
    log("building cuebot (gradle assemble; warms the cache for bootRun) ...")
    cmd = ["./gradlew", "assemble", "-g", GHOME, "--no-daemon", "--console=plain"]
    if JDK17 and os.path.isdir(JDK17):
        cmd.append(f"-Dorg.gradle.java.home={JDK17}")
    env = dict(os.environ)
    env.pop("JAVA_TOOL_OPTIONS", None)   # NO private hosts file during the build
    r = subprocess.run(cmd, cwd=CUEBOT_DIR, env=env,
                       capture_output=True, text=True, timeout=1800)
    if r.returncode != 0:
        sys.exit(f"cuebot build (assemble) failed:\n{(r.stderr or r.stdout)[-1500:]}")
    log("  cuebot build ready (cache warm)")


def start_cuebot(mode, reservations=False, block_seconds=60, max_fraction=0.5,
                 max_grantees=8, backfill=True):
    enabled = "true" if mode == "new" else "false"
    resv = "true" if reservations else "false"
    bf = "true" if backfill else "false"
    log(f"starting cuebot (mode={mode}, scheduler.enabled={enabled}, "
        f"reservations={resv}, block={block_seconds}s, max_frac={max_fraction}, "
        f"max_grantees={max_grantees}, backfill={bf}) ...")
    # Point cuebot's JVM at our private hosts file so every farm name resolves
    # to 127.0.0.1 (where fake_rqd listens) without touching /etc/hosts. The
    # forked bootRun application JVM inherits JAVA_TOOL_OPTIONS from this env, so
    # the property is in effect before InetAddress initialises.
    java_tool_opts = f"-Djdk.net.hosts.file={SIM_HOSTS_FILE}"
    if os.environ.get("JAVA_TOOL_OPTIONS"):
        java_tool_opts = os.environ["JAVA_TOOL_OPTIONS"] + " " + java_tool_opts
    env = dict(os.environ)
    env.update({
        "JAVA_TOOL_OPTIONS": java_tool_opts,
        "CUEBOT_DB_URL": f"jdbc:postgresql://127.0.0.1:{PG_PORT}/cuebot",
        "CUEBOT_DB_USER": "cue", "CUEBOT_DB_PASSWORD": "",
        "SCHEDULER_ENABLED": enabled,
        "SCHEDULER_INTERVAL_MS": os.environ.get("SIM_TICK_MS", "3000"),
        "SCHEDULER_RESERVATIONS_ENABLED": resv,
        "SCHEDULER_RESERVATION_BLOCK_SECONDS": str(block_seconds),
        "SCHEDULER_RESERVATION_MAX_FRACTION": str(max_fraction),
        "SCHEDULER_RESERVATION_MAX_GRANTEES": str(max_grantees),
        "SCHEDULER_BACKFILL_ENABLED": bf,
    })
    # Runs as the current (non-root) user — no sudo. --no-daemon: a reused
    # Gradle daemon could carry a stale environment and fork the app JVM without
    # our JAVA_TOOL_OPTIONS (so the hosts file wouldn't take effect). Running
    # daemonless guarantees the launcher we start owns the fork and the property
    # is in effect.
    cmd = ["./gradlew", "bootRun", "-g", GHOME, "--no-daemon", "--console=plain"]
    # Pin the JDK only if SIM_JDK_HOME points at a real dir; otherwise let gradle
    # use the ambient JAVA_HOME (more portable on machines with JDK17 on PATH).
    if JDK17 and os.path.isdir(JDK17):
        cmd.append(f"-Dorg.gradle.java.home={JDK17}")
    logf = open(CUEBOT_LOG, "w")
    subprocess.Popen(cmd, cwd=CUEBOT_DIR, stdout=logf, stderr=subprocess.STDOUT,
                     start_new_session=True, env=env)
    # Wait for ready.
    for i in range(180):
        if "Started CuebotApplication" in read_text(CUEBOT_LOG) and port_open(GRPC_PORT):
            log(f"  cuebot ready after ~{i}s (gRPC :{GRPC_PORT})")
            return
        time.sleep(1)
    sys.exit(f"cuebot did not become ready; see {CUEBOT_LOG}")


def start_extra_cuebot(instance, mode, reservations=False, block_seconds=60,
                       max_fraction=0.5, max_grantees=8, backfill=True):
    """Launch an ADDITIONAL cuebot (instance >= 1) from the built jar, on offset
    ports, against the SAME Postgres with scheduler.enabled. All instances race
    for the Postgres advisory lock each tick, so exactly one plans at a time:
    this is how the sim exercises leader election / HA (Scheduler.md section 4).

    Only instance 0 (start_cuebot, via bootRun) is reached by the farm, fake RQD
    and feeder; the extras just join to plan from the shared DB and fire their
    RQD launches through the same hosts file. Using the prebuilt jar (not a second
    `gradlew bootRun`) avoids two Gradle builds fighting over the build dir."""
    cue = GRPC_PORT + 10 * instance     # 8443, 8453, 8463, ...
    rqd = cue + 1                       # 8444, 8454, ...
    web = 8080 + instance               # embedded Tomcat (metrics); 8081, 8082, ...
    logpath = CUEBOT_LOG.replace(".log", f"-{instance}.log")
    jar = os.path.join(CUEBOT_DIR, "build", "libs", "cuebot.jar")
    if not os.path.exists(jar):
        sys.exit(f"cuebot jar not found at {jar} (ensure_cuebot_built should have built it)")
    enabled = "true" if mode == "new" else "false"
    java_tool_opts = f"-Djdk.net.hosts.file={SIM_HOSTS_FILE}"
    if os.environ.get("JAVA_TOOL_OPTIONS"):
        java_tool_opts = os.environ["JAVA_TOOL_OPTIONS"] + " " + java_tool_opts
    env = dict(os.environ)
    env.update({
        "JAVA_TOOL_OPTIONS": java_tool_opts,
        "CUEBOT_DB_URL": f"jdbc:postgresql://127.0.0.1:{PG_PORT}/cuebot",
        "CUEBOT_DB_USER": "cue", "CUEBOT_DB_PASSWORD": "",
        "SCHEDULER_ENABLED": enabled,
        "SCHEDULER_INTERVAL_MS": os.environ.get("SIM_TICK_MS", "3000"),
        "SCHEDULER_RESERVATIONS_ENABLED": "true" if reservations else "false",
        "SCHEDULER_RESERVATION_BLOCK_SECONDS": str(block_seconds),
        "SCHEDULER_RESERVATION_MAX_FRACTION": str(max_fraction),
        "SCHEDULER_RESERVATION_MAX_GRANTEES": str(max_grantees),
        "SCHEDULER_BACKFILL_ENABLED": "true" if backfill else "false",
        # Offset every listener so the extra never collides with instance 0.
        "CUEBOT_GRPC_CUE_PORT": str(cue),
        "CUEBOT_GRPC_RQD_SERVER_PORT": str(rqd),
        "SERVER_PORT": str(web),
    })
    java = os.path.join(JDK17, "bin", "java") if (JDK17 and os.path.isdir(JDK17)) else "java"
    log(f"starting cuebot #{instance} (jar, gRPC :{cue}, web :{web}, "
        f"scheduler.enabled={enabled}) ...")
    logf = open(logpath, "w")
    subprocess.Popen([java, "-jar", jar], cwd=CUEBOT_DIR, stdout=logf,
                     stderr=subprocess.STDOUT, start_new_session=True, env=env)
    for i in range(180):
        if "Started CuebotApplication" in read_text(logpath) and port_open(cue):
            log(f"  cuebot #{instance} ready after ~{i}s (gRPC :{cue})")
            return
        time.sleep(1)
    sys.exit(f"cuebot #{instance} did not become ready; see {logpath}")


# ---------------------------------------------------------------- helpers
def write_sim_hosts_file():
    """Write a private JVM hosts file mapping every farm host → 127.0.0.1.

    cuebot resolves each host name to dial its RQD; we point cuebot's JVM at
    THIS file via -Djdk.net.hosts.file (see start_cuebot) so no root / no
    /etc/hosts edit is needed. Writes the active farm set (honors --hosts /
    SIM_HOST_COUNTS — exactly the hosts that get registered and dialed).
    localhost and the local machine name are included because this file becomes
    the JVM's ONLY name source.
    """
    sys.path.insert(0, FARM)
    import farm_spec
    import importlib
    importlib.reload(farm_spec)  # honor SIM_HOST_COUNTS if set
    names = sorted({n for n, _, _ in farm_spec.all_hosts()})
    lines = ["127.0.0.1 localhost", "::1 localhost"]
    try:
        lines.append(f"127.0.0.1 {socket.gethostname()}")
    except Exception:
        pass
    lines += [f"127.0.0.1 {n}" for n in names]
    with open(SIM_HOSTS_FILE, "w") as f:
        f.write("\n".join(lines) + "\n")
    os.chmod(SIM_HOSTS_FILE, 0o644)  # plain readable; cuebot runs as this same user
    log(f"  wrote JVM hosts file {SIM_HOSTS_FILE} ({len(names)} farm hosts → 127.0.0.1)")


def spawn(script_args, logpath, env_extra=None):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    logf = open(logpath, "w")
    return subprocess.Popen([VENV_PY] + script_args, cwd=FARM,
                            stdout=logf, stderr=subprocess.STDOUT,
                            start_new_session=True, env=env)


def ensure_hosts(nhosts, expected):
    if nhosts >= expected:
        log(f"hosts present ({nhosts})")
        return
    log(f"have {nhosts}/{expected} hosts; registering the farm ...")
    r = sh([VENV_PY, "register_hosts.py"], cwd=FARM, timeout=300)
    log("  " + (r.stdout.strip().splitlines() or ["registered"])[-1])


def start_fake_rqd(threads, mem_failure_rate=0.0):
    log(f"starting fake RQD (reporter threads={threads}"
        + (f", mem_failure_rate={mem_failure_rate:.0%}" if mem_failure_rate > 0 else "")
        + ") ...")
    spawn(["fake_rqd.py", str(threads), str(mem_failure_rate)], RQD_LOG)
    for _ in range(30):
        log_txt = read_text(RQD_LOG)
        if "listening" in log_txt:
            log("  fake RQD up")
            return
        if "Address already in use" in log_txt:
            log("  WARN fake RQD: port 8444 already in use (stale process?); "
                "see rqd.log")
            return
        time.sleep(1)
    log("  WARN fake RQD startup not confirmed (see rqd.log)")


def start_pinger(interval, expected):
    log(f"starting host+frame status reporter (rqd_report, interval={interval}s) ...")
    spawn(["rqd_report.py", str(interval)], PINGER_LOG)
    for _ in range(20):
        up = psql("SELECT count(*) FROM host_stat WHERE ts_ping > now() - interval '15 seconds';")
        try:
            if int(up.stdout.strip() or 0) >= expected:
                log(f"  {expected} hosts UP")
                return
        except ValueError:
            pass
        time.sleep(2)
    log("  WARN not all hosts confirmed UP yet (pinger keeps trying)")


# ---------------------------------------------------------------- workload
def submit_jobs(njobs, seed):
    log(f"submitting {njobs} deterministic jobs (seed base {seed}) ...")
    code = (
        f"import sys,random; sys.path.insert(0,{FARM + '/opencue_proto'!r});"
        f"sys.path.insert(0,{FARM!r});"
        "import grpc,job_pb2,job_pb2_grpc,gen_jobs,farm_spec;"
        "ch=grpc.insecure_channel(farm_spec.GRPC);"
        "grpc.channel_ready_future(ch).result(timeout=15);"
        "st=job_pb2_grpc.JobInterfaceStub(ch);"
        f"[ (random.seed({seed}+i), st.LaunchSpec(job_pb2.JobLaunchSpecRequest("
        "spec=gen_jobs.SPEC_HEAD+gen_jobs.make_job(i)+'</spec>\\n'))) "
        f"for i in range({njobs}) ];"
        "print('submitted')")
    r = sh([VENV_PY, "-c", code], cwd=FARM, timeout=300)
    log("  " + (r.stdout.strip() or r.stderr.strip()[-200:] or "done"))


def start_feeder(duration, target):
    log(f"starting paced feeder (hold ~{target} waiting frames for {duration}s) ...")
    spawn(["feed.py", str(duration), str(target)], FEED_LOG)


def start_big_injector(duration, interval):
    log(f"starting BIG-job injector (64-core jobs every {interval}s for "
        f"{duration}s, mixed equal/high priority) ...")
    spawn(["inject_big.py", str(duration), str(interval)], f"{FARM}/inject_big.log")


# ---------------------------------------------------------------- main
def main():
    # If we're root, drop to a non-root user and re-run (postgres/cuebot refuse
    # root). For a normal user this is a no-op and nothing uses sudo.
    reexec_as_nonroot_if_needed()
    # Make sure we're under an interpreter that has grpc (re-exec under the
    # sim's venv if not), so the helpers we spawn never die at `import grpc`.
    ensure_grpc_interpreter()
    ensure_buildable()
    ensure_proto_stubs()
    ap = argparse.ArgumentParser(description="One-command fresh scheduler sim")
    ap.add_argument("--mode", choices=["new", "old"], default="new")
    ap.add_argument("--cuebots", type=int, default=2, metavar="N",
                    help="number of cuebot instances to run against the same "
                         "Postgres (default 2). All race the advisory lock so one "
                         "plans per tick: this exercises leader election / HA. Set "
                         "1 for a single-cuebot run. Instance 0 runs via bootRun and "
                         "owns the gRPC/RQD/feeder traffic; extras run from the jar "
                         "on offset ports and only join to plan.")
    ap.add_argument("--jobs", type=int, default=0,
                    help="submit N fixed deterministic jobs at start")
    ap.add_argument("--seed", type=int, default=9000)
    ap.add_argument("--feed", type=int, default=0,
                    help="run paced feeder for SECONDS (sustained backlog)")
    ap.add_argument("--feed-target", type=int, default=40000)
    ap.add_argument("--reporter-threads", type=int, default=1,
                    help="fake_rqd completion-report concurrency (default 1)")
    ap.add_argument("--compress", type=float, default=None,
                    help="sim duration compression (SIM_COMPRESS); higher=longer "
                         "frames=lower lifecycle rate. Default uses sim_model's 0.27")
    ap.add_argument("--metrics", type=int, default=0,
                    help="run metrics.py for SECONDS (reserved-util view)")
    ap.add_argument("--stats", type=int, default=0,
                    help="run stats.py for SECONDS: honest LIVE util (cores "
                         "backing RUNNING frames), zombie-proc leak detector, "
                         "per-type breakdown, steady-state averages")
    ap.add_argument("--heartbeat-interval", type=float, default=0.1,
                    help="seconds the reporter SLEEPS between full report rounds "
                         "(default 0.1 = report continuously). This is critical for "
                         "OLD/legacy mode: the report-driven booker only books a host "
                         "WHEN it reports, so a slow heartbeat starves it of booking "
                         "opportunities and the farm never fills. A real farm has "
                         "thousands of RQDs reporting constantly; 0.1 mimics that. "
                         "Also keeps proc.ts_ping fresh for the 300s orphan sweep.")
    ap.add_argument("--hosts", type=str, default=None,
                    help="SMALL-FARM debug mode: 'jaime,ram,elk' counts, e.g. "
                         "'2,3,5'. Shrinks the farm (removes all other hosts and "
                         "re-registers just this set) so booking dynamics are easy "
                         "to watch. Default: full 1553-host farm.")
    ap.add_argument("--tags", action="store_true",
                    help="capability tags (production routing): each host "
                         "advertises the size classes it can run (small/med/large "
                         "= elk/ram/jaime; a big box runs anything smaller) and "
                         "each LAYER is tagged with the smallest class whose "
                         "machines fit it. Big layers are confined to big machines; "
                         "small work still packs onto big machines. Default off.")
    ap.add_argument("--gpu", type=float, default=0.0, metavar="F",
                    help="GPU pools: fraction F of the farm is GPU-capable (drawn "
                         "only from ram/elk hosts) and fraction F of layers are GPU "
                         "layers (4 cores + 1 GPU + gpu_memory, cpu mem = half the "
                         "gpu mem). GPU layers place only on GPU hosts (enforced by "
                         "cuebot). Default 0 (no GPU). Typical: 0.1.")
    ap.add_argument("--reservations", action="store_true",
                    help="enable the planner's host reservations for blocked "
                         "layers (EASY/Maui-style: time gate + per-class cap). "
                         "Default off. Use with --strand to show big jobs no "
                         "longer starve.")
    ap.add_argument("--reservation-block-seconds", type=int, default=60,
                    metavar="SECS",
                    help="how long a layer must be continuously blocked before "
                         "it may reserve (default 60 for the sim; production "
                         "default is 300 = 5 min).")
    ap.add_argument("--reservation-max-fraction", type=float, default=0.5,
                    metavar="F",
                    help="reservations may hold at most this fraction of the "
                         "hosts that can fit a layer (default 0.5), so a host "
                         "class is never fully reserved.")
    ap.add_argument("--reservation-max-grantees", type=int, default=8,
                    metavar="K",
                    help="maximum new reservation grants per tick (default 8). "
                         "Caps O(layers x hosts) work at full-farm scale; "
                         "existing holders always reconcile regardless of K.")
    ap.add_argument("--no-backfill", dest="backfill", action="store_false",
                    help="disable EASY backfill (Lifka 1995). By default a "
                         "reserved host lets short lower-priority frames run on "
                         "its free cores while it drains, as long as they finish "
                         "before the reserved (wide) job needs it. Disabling "
                         "freezes reserved hosts idle until they drain.")
    ap.set_defaults(backfill=True)
    ap.add_argument("--strand", type=int, default=0, metavar="SECS",
                    help="STARVATION TEST: alongside the small feeder, inject "
                         "64-core BIG jobs every --strand-interval seconds "
                         "(mixed equal/high priority) and watch them for SECS. "
                         "Without reservations the big jobs strand: jaime stays "
                         "packed with small frames and never frees 64 cores at "
                         "once. Needs --feed for the small backlog.")
    ap.add_argument("--strand-interval", type=int, default=30, metavar="SECS",
                    help="seconds between BIG-job injections (default 30, "
                         "'once in a while' like a real facility)")
    ap.add_argument("--mem-failure-rate", type=float, default=0.0,
                    help="fraction of frame completions that report exit_status=33 "
                         "(memory failure). Cuebot then bumps the layer memory by ~2 GB "
                         "and requeues the frame. Default 0 (no failures). "
                         "Example: 0.1 = 10%% of frames fail once with OOM.")
    ap.add_argument("--mem-heavy", type=float, nargs="?", const=24.0, default=0.0,
                    metavar="GB",
                    help="LOW-UTILIZATION TEST: flood the farm with small, "
                         "memory-hungry jobs. Layers are drawn only from the small "
                         "buckets (<= --mem-heavy-max-cores cores) and made to "
                         "demand GB memory on average (bare flag defaults to 24), "
                         "so memory, not cores, becomes the binding constraint. "
                         "Hosts run ~3.5-3.9 GB/core, so e.g. 32 exhausts host RAM "
                         "with most cores still idle and utilization plateaus well "
                         "below 100%%. Off by default.")
    ap.add_argument("--mem-heavy-max-cores", type=int, default=4, metavar="N",
                    help="with --mem-heavy, the largest core count treated as a "
                         "'small' (memory-heavy) layer (default 4).")
    args = ap.parse_args()

    if args.compress is not None:
        os.environ["SIM_COMPRESS"] = str(args.compress)  # inherited by feeder/rqd
    if args.hosts:
        os.environ["SIM_HOST_COUNTS"] = args.hosts  # inherited by all helpers
    if args.tags:
        os.environ["SIM_TAGS"] = "1"  # inherited by all helpers
    if args.gpu:
        os.environ["SIM_GPU"] = str(args.gpu)  # inherited by all helpers
    if args.mem_heavy > 0:
        os.environ["SIM_MEM_HEAVY_GB"] = str(args.mem_heavy)  # inherited by feeder
        os.environ["SIM_MEM_HEAVY_MAX_CORES"] = str(args.mem_heavy_max_cores)
        log(f"MEM-HEAVY low-util test: layers <= {args.mem_heavy_max_cores} cores "
            f"average {args.mem_heavy:g} GB (memory-bound; cores will strand)")

    # Expected host count: small farm honors --hosts, else the full farm.
    sys.path.insert(0, FARM)
    import farm_spec
    import importlib
    importlib.reload(farm_spec)  # pick up SIM_HOST_COUNTS set above
    expected_hosts = farm_spec.total_hosts()
    if args.hosts:
        log(f"SMALL-FARM mode: {expected_hosts} hosts "
            f"({farm_spec.total_cores()} cores) — counts {args.hosts}")
    if args.tags:
        from collections import Counter
        hc = Counter(farm_spec.host_tags(n)[-1] for n, _, _ in farm_spec.all_hosts())
        log(f"CAPABILITY TAGS on — host top-class split: {dict(sorted(hc.items()))} "
            f"(each host also carries all smaller classes; layers tagged to the "
            f"smallest class that fits)")
    if args.gpu:
        gpu_hosts = sum(1 for n, c, m in farm_spec.all_hosts()
                        if farm_spec.host_gpu(n, c, m)[0] > 0)
        gpu_units = sum(farm_spec.host_gpu(n, c, m)[0] for n, c, m in farm_spec.all_hosts())
        log(f"GPU: {args.gpu:.0%} of farm GPU-capable — {gpu_hosts}/{expected_hosts} "
            f"hosts ({gpu_units} GPUs total), {args.gpu:.0%} of layers are GPU layers")

    t0 = time.time()
    teardown()
    ensure_postgres()
    write_sim_hosts_file()
    nhosts = reset_db()
    ensure_cuebot_built()
    start_cuebot(args.mode, args.reservations,
                 args.reservation_block_seconds, args.reservation_max_fraction,
                 args.reservation_max_grantees, args.backfill)
    for i in range(1, max(1, args.cuebots)):
        start_extra_cuebot(i, args.mode, args.reservations,
                           args.reservation_block_seconds, args.reservation_max_fraction,
                           args.reservation_max_grantees, args.backfill)
    if args.cuebots > 1:
        log(f"{args.cuebots} cuebots up, sharing the Postgres advisory lock "
            f"(one plans per tick)")
    ensure_hosts(nhosts, expected_hosts)
    start_fake_rqd(args.reporter_threads, args.mem_failure_rate)
    start_pinger(args.heartbeat_interval, expected_hosts)

    if args.jobs:
        submit_jobs(args.jobs, args.seed)
    if args.feed:
        start_feeder(args.feed, args.feed_target)
    if args.strand:
        start_big_injector(args.strand, args.strand_interval)

    log(f"stack is UP and fresh in {time.time()-t0:.0f}s  "
        f"(mode={args.mode}).")
    log(f"logs: cuebot={CUEBOT_LOG}  rqd={RQD_LOG}  pinger={PINGER_LOG}"
        + (f"  feed={FEED_LOG}" if args.feed else ""))

    # Live consolidated stats (util, frames/s, DB, and BIG-job/stranded when
    # big jobs are present) stream by default for the duration of whatever phase
    # is active, so a run can be watched without querying the DB by hand.
    watch = args.strand or args.stats or args.metrics or args.feed
    if args.strand:
        log(f"watching BIG-job stranding + live stats for {args.strand}s "
            f"(small feeder runs alongside) ...")
        subprocess.run([VENV_PY, "live_stats.py", str(args.strand), "5"], cwd=FARM)
    elif watch:
        secs = args.stats or args.metrics or args.feed
        log(f"streaming live stats for {secs}s ...")
        subprocess.run([VENV_PY, "live_stats.py", str(secs), "5"], cwd=FARM)
    else:
        log(f"run live stats any time:  {VENV_PY} {os.path.join(FARM, 'live_stats.py')} 120")

    # DB load summary by default, after whatever phase ran above.
    db_stats()


if __name__ == "__main__":
    main()
