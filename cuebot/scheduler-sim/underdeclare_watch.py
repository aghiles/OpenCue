"""UNDERDECLARE verdict: do under-declared layers stop OOM-churning once the
farm has seen their real rss?

Companion to inject_underdeclare.py. Both flood layers declare 4G and really
hold 18G (the fake RQD pins their reported rss). Placement trusts the ledger,
not the declaration, so:
  - pre-evidence bookings reserve the declared 4G, hosts oversubscribe, spill
    to swap, and cuebot's host-OOM logic kills the hogs (the disease -- real
    cuebot-initiated kills, counted from the cuebot log);
  - once the ledger has its samples, every later placement reserves the true
    18G and books the metric core share (500 points), so the kills STOP.

Judged on the persistent record + the cuebot log:
  - PASS: kills happened (the disease was provoked) but stopped (none in the
    final window); the 1-core arm's ask-sized wave stayed inside the probe
    gate; the 4-core arm's ask-sized wave stayed a pre-evidence burst (not
    the whole run); late launches of BOTH arms carry 500 points; late procs
    reserve the real 18G; the honest non-threadable control was never
    resized and never killed.
  - FAIL: kills keep coming (declaration still trusted), a wave never
    converged, or the control was harmed.
  - INCONCLUSIVE: not enough started frames, or the disease never provoked
    a single kill (nothing to judge).

usage: underdeclare_watch.py [duration_s] [interval_s]
"""
import glob
import os, re, sys, time, subprocess
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import farm_spec as spec

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 240
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
PSQL = spec.psql_cmd()
CUEBOT_LOG = os.environ.get("SIM_CUEBOT_LOG", "/tmp/cuebot.log")


def cuebot_logs():
    """Every cuebot instance's log: the leader (and so the killer) can be any
    instance, and extras log to <base>-N.log."""
    base = CUEBOT_LOG
    return sorted(set(glob.glob(base) + glob.glob(base.replace(".log", "-*.log"))))
EXP = 500                 # 18G over the farm's uniform 4G/core -> 5 cores
MIN_STARTED = 60          # flood frames (both arms) needed to judge
LATE_N = 50               # last-started frames that must carry the grant
MAX_PROBE = 40            # 1-core ask wave must stay inside the probe gate
MAX_WAVE4 = 600           # 4-core ask wave must be one burst (~520 slots
                          # on the 40-small farm), not the whole run
LATE_KILL_WIN = 90.0      # seconds at the end that must be kill-free
RESV_HONEST_KB = 17_000_000   # late procs must reserve ~the real 18G


def rows(sql):
    try:
        out = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True,
                             timeout=15).stdout.strip()
        return [ln.split("|") for ln in out.splitlines() if ln]
    except Exception:
        return []


def hist(token):
    r = rows(f"SELECT f.int_cores, count(*) FROM frame f "
             f"JOIN job j ON j.pk_job = f.pk_job "
             f"WHERE j.str_name LIKE '%{token}%' AND f.ts_started IS NOT NULL "
             f"GROUP BY 1;")
    return {int(a): int(b) for a, b in r}


def late_hist(token, n):
    r = rows(f"SELECT c, count(*) FROM (SELECT f.int_cores AS c FROM frame f "
             f"JOIN job j ON j.pk_job = f.pk_job "
             f"WHERE j.str_name LIKE '%{token}%' AND f.ts_started IS NOT NULL "
             f"ORDER BY f.ts_started DESC LIMIT {n}) t GROUP BY 1;")
    return {int(a): int(b) for a, b in r}


def late_joint(token, n):
    """(cores, reserved GB) pairs of the last n started frames. The joint view
    tells resize-with-clamped-cores (400cp, 18G) apart from no-resize-at-all
    (400cp, 4G); the separate histograms cannot."""
    r = rows(f"SELECT c, g, count(*) FROM (SELECT f.int_cores AS c, "
             f"round(f.int_mem_reserved / 1048576.0) AS g FROM frame f "
             f"JOIN job j ON j.pk_job = f.pk_job "
             f"WHERE j.str_name LIKE '%{token}%' AND f.ts_started IS NOT NULL "
             f"ORDER BY f.ts_started DESC LIMIT {n}) t GROUP BY 1, 2;")
    return {(int(a), int(float(b))): int(c) for a, b, c in r}


def kill_counts():
    """Cuebot-initiated memory kills per job, from the cuebot log."""
    per = {"flood1": 0, "flood4": 0, "ctrl": 0, "other": 0}
    for path in cuebot_logs():
        try:
            with open(path, errors="ignore") as f:
                for ln in f:
                    if "Killing frame on" not in ln:
                        continue
                    m = re.search(r"simunderdecl_(flood1|flood4|ctrl)", ln)
                    per[m.group(1) if m else "other"] += 1
        except Exception:
            pass
    return per


def bump_count():
    n = 0
    for path in cuebot_logs():
        try:
            with open(path, errors="ignore") as f:
                n += sum(1 for ln in f if "per-frame mem bump to:" in ln)
        except Exception:
            pass
    return n


def late_share(h, n_expected):
    n = sum(h.values())
    return ((100.0 * h.get(EXP, 0) / n) if n else 0.0), n


def main():
    print(f"watching UNDERDECLARE for {DURATION}s: 4G-declared frames really "
          f"holding 18G must provoke host-OOM kills only until the rss ledger "
          f"converges; late launches must reserve 18G and book {EXP} points "
          f"(1-core arm probe-gated, 4-core arm never gated, only corrected).\n",
          flush=True)
    t0 = time.time()
    kill_series = []          # (elapsed_s, total_kills)
    while time.time() - t0 < DURATION:
        t = time.time() - t0
        u = rows("SELECT sum(int_cores), sum(int_cores - int_cores_idle) "
                 "FROM host;")
        util = 0.0
        if u and int(u[0][0] or 0) > 0:
            util = 100.0 * int(u[0][1] or 0) / int(u[0][0])
        k = kill_counts()
        total = sum(k.values())
        kill_series.append((t, total))
        live = rows("SELECT p.int_cores_reserved, count(*) FROM proc p "
                    "JOIN job j ON j.pk_job = p.pk_job "
                    "WHERE j.str_name LIKE '%simunderdecl%' GROUP BY 1 "
                    "ORDER BY 1;")
        dist = " ".join(f"{int(a) // 100}c x{b}" for a, b in live) or "none"
        print(f"t={t:5.0f} | util {util:5.1f}% | kills {total} "
              f"(f1 {k['flood1']} f4 {k['flood4']} ctrl {k['ctrl']}) | "
              f"live: {dist}", flush=True)
        time.sleep(INTERVAL)

    h1 = hist("simunderdecl_flood1")
    h4 = hist("simunderdecl_flood4")
    ctrl = hist("simunderdecl_ctrl")
    l1, l1n = late_share(late_hist("simunderdecl_flood1", LATE_N), LATE_N)
    l4, l4n = late_share(late_hist("simunderdecl_flood4", LATE_N), LATE_N)
    started = sum(h1.values()) + sum(h4.values())
    kills = kill_counts()
    ktotal = sum(kills.values())
    cutoff = (time.time() - t0) - LATE_KILL_WIN
    base = 0
    for t, c in kill_series:
        if t <= cutoff:
            base = c
    late_kills = ktotal - base
    probe = h1.get(100, 0)
    wave4 = h4.get(400, 0)
    ctrl_max = max(ctrl) if ctrl else 0
    r = rows(f"SELECT count(*) FILTER (WHERE p.int_mem_reserved >= "
             f"{RESV_HONEST_KB}), count(*) FROM proc p "
             f"JOIN job j ON j.pk_job = p.pk_job "
             f"WHERE j.str_name LIKE '%simunderdecl_flood%' "
             f"AND p.ts_booked > now() - interval '{int(LATE_KILL_WIN)} "
             f"seconds';")
    resv_ok, resv_n = (int(r[0][0] or 0), int(r[0][1] or 0)) if r else (0, 0)
    resv_pct = 100.0 * resv_ok / resv_n if resv_n else 100.0
    bumps = bump_count()

    print("\n==== UNDERDECLARE VERDICT ====", flush=True)
    print(f"hist f1 {sorted(h1.items())}; hist f4 {sorted(h4.items())}; "
          f"late50 f1 {sorted(late_hist('simunderdecl_flood1', LATE_N).items())} "
          f"(n={l1n}); late50 f4 "
          f"{sorted(late_hist('simunderdecl_flood4', LATE_N).items())} (n={l4n})",
          flush=True)
    j1 = late_joint("simunderdecl_flood1", LATE_N)
    j4 = late_joint("simunderdecl_flood4", LATE_N)
    print(f"late joint (cores, resvG) f1 {sorted(j1.items())}; f4 "
          f"{sorted(j4.items())}", flush=True)
    # Late launches come in two legitimate shapes: the resized 500-point
    # figure, and squeeze slices below it (one per near-fit hole). Whatever
    # the cores, the memory must carry the evidence: nothing under ~17G.
    lowmem1 = sum(n for (cp, g), n in j1.items() if g < 17)
    lowmem4 = sum(n for (cp, g), n in j4.items() if g < 17)
    print(f"kills total {ktotal} (flood1 {kills['flood1']}, flood4 "
          f"{kills['flood4']}, ctrl {kills['ctrl']}), late90 {late_kills}; "
          f"probe {probe}; wave4 {wave4}; late50 at {EXP}: f1 {l1:.0f}% "
          f"f4 {l4:.0f}%; late resv>=17G {resv_pct:.0f}% of {resv_n}; "
          f"bumps {bumps}; started {started}", flush=True)
    if started < MIN_STARTED:
        print(f"INCONCLUSIVE: only {started} flood frames ever started "
              f"(under {MIN_STARTED}); nothing to judge.", flush=True)
    elif ktotal == 0:
        print("INCONCLUSIVE: the disease never provoked a single host-OOM "
              "kill; nothing to judge about convergence.", flush=True)
    elif kills["ctrl"] > 0 or ctrl_max > 100:
        print(f"FAIL: the honest non-threadable control was harmed (kills "
              f"{kills['ctrl']}, max cores {ctrl_max}).", flush=True)
    elif late_kills > 0:
        print(f"FAIL: {late_kills} kills in the final {LATE_KILL_WIN:.0f}s; "
              f"the declaration is still being trusted and the massacre "
              f"repeats.", flush=True)
    elif probe > MAX_PROBE:
        print(f"FAIL: {probe} 1-core-arm frames booked at the ask (allowed "
              f"{MAX_PROBE}); the probe gate leaks.", flush=True)
    elif wave4 > MAX_WAVE4:
        print(f"FAIL: {wave4} 4-core-arm frames booked at the ask (allowed "
              f"{MAX_WAVE4}); the explicit-ask arm never converged.",
              flush=True)
    elif lowmem1 > 0 or lowmem4 > 0:
        print(f"FAIL: {lowmem1 + lowmem4} late launches reserve under 17G; "
              f"a booking escaped the rss evidence.", flush=True)
    elif l1 < 40.0 or l4 < 40.0:
        print(f"FAIL: the {EXP}-point share of late launches collapsed "
              f"(f1 {l1:.0f}%, f4 {l4:.0f}%); the core resize is not acting.",
              flush=True)
    elif resv_n >= 5 and resv_pct < 80.0:
        print(f"FAIL: late procs still reserve the declared 4G "
              f"({resv_pct:.0f}% honest of {resv_n}); memory was not "
              f"corrected.", flush=True)
    else:
        print(f"PASS: {ktotal} kills all inside the pre-evidence window "
              f"(0 in the last {LATE_KILL_WIN:.0f}s), probe {probe}, "
              f"4-core burst {wave4}, every late launch >= 17G with the "
              f"{EXP}-pt share at f1 {l1:.0f}% f4 {l4:.0f}% (squeeze slices "
              f"allowed), late reservations honest ({resv_pct:.0f}%), "
              f"control untouched.", flush=True)


if __name__ == "__main__":
    main()
