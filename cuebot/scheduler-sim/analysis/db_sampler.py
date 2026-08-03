"""Sample postgres activity over time -> CSV, for plot_run.py.

Every 2s records the cuebot database's cumulative counters from
pg_stat_database (commits, rollbacks, tuples returned/fetched/inserted/updated/
deleted, deadlocks, block reads vs cache hits) alongside two point-in-time
gauges from pg_stat_activity: sessions currently active, and sessions blocked
on a lock. The counters are cumulative, so read them as slopes -- make_graphs.py
differences consecutive rows into per-second rates. This is the file to look at
when throughput stalls but CPU is idle: a climbing lockwait or deadlock count
means the scheduler is contending with itself, not running out of machine.
Honors SIM_PG_HOST / SIM_PG_PORT / SIM_PG_BIN.

usage: db_sampler.py <out.csv>
"""
import time, subprocess, sys, os
OUT = sys.argv[1]
_PORT = os.environ.get("SIM_PG_PORT", "5433")
_HOST = os.environ.get("SIM_PG_HOST", "127.0.0.1")
_PSQL_BIN = os.path.join(os.environ.get("SIM_PG_BIN", "/usr/lib/postgresql/16/bin"), "psql")
PSQL = [_PSQL_BIN,"-tA","-h",_HOST,"-p",_PORT,"-U","cue","-d","cuebot","-c"]
def q(sql):
    """Run one SQL statement through psql and return stripped stdout.

    Errors are deliberately swallowed to an empty string: the sampler must
    outlive a database that is still starting up or briefly unreachable, and
    the caller already skips rows that don't parse.
    """
    return subprocess.run(PSQL+[sql], capture_output=True, text=True).stdout.strip()
with open(OUT,"w") as f:
    f.write("ts,commits,rollbacks,tup_ret,tup_fetch,ins,upd,del,deadlocks,"
            "blks_read,blks_hit,active,lockwait\n"); f.flush()
    while True:
        db = q("SELECT xact_commit||','||xact_rollback||','||tup_returned||','||tup_fetched"
               "||','||tup_inserted||','||tup_updated||','||tup_deleted||','||deadlocks"
               "||','||blks_read||','||blks_hit "
               "FROM pg_stat_database WHERE datname='cuebot'")
        act = q("SELECT count(*) FILTER (WHERE state='active')||','||"
                "count(*) FILTER (WHERE wait_event_type='Lock') "
                "FROM pg_stat_activity WHERE datname='cuebot' AND pid<>pg_backend_pid()")
        if db and act:
            f.write(f"{time.strftime('%H:%M:%S')},{db},{act}\n"); f.flush()
        time.sleep(2)
