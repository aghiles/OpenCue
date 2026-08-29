"""SQUEEZE_FLAT watcher: flat-memory squeezes must not start kill cycles.

Classifies each host by the tenant actually running on it (tena = memory-
short hole, tenb = cores-short hole, none = seed), then samples the flood's
procs, the cuebot kill log, and the flood layer's memory ask.

On the unmodified build the memory-short squeeze under-reserves (46.4G for
a flat 50G footprint), hosts tip over physical memory, and the host-OOM
balancer kills in a cycle; the layer OOM escalation then ratchets the ask.
That is the disease and the watcher reports FAIL for it.

PASS (the fixed build): zero kills, zero squeezes into memory-short holes,
at least 2 squeezed bookings in cores-short holes (the safe class still
works), and the flood layer's ask never ratcheted off its declaration.
INCONCLUSIVE: the flood never took hold.

usage: squeeze_flat_watch.py [duration_s] [interval_s]
"""
import glob, os, re, subprocess, sys, time
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import farm_spec as spec

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
ASK_CP = 1400
ASK_MEM_KB = 51200 * 1024
MIN_KILLS_DISEASE = 4
PSQL = spec.psql_cmd()


def rows(sql):
    try:
        out = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True,
                             timeout=15).stdout.strip()
        return [ln.split("|") for ln in out.splitlines() if ln]
    except Exception:
        return []


def cuebot_logs():
    base = os.environ.get("SIM_CUEBOT_LOG", "/tmp/cuebot-new.log")
    root, ext = os.path.splitext(base)
    return [p for p in [base] + sorted(glob.glob(f"{root}-*{ext}"))
            if os.path.exists(p)]


def kill_count():
    n = 0
    for path in cuebot_logs():
        try:
            with open(path, errors="ignore") as f:
                for ln in f:
                    if "Killing frame on" in ln and "simfltx" in ln:
                        n += 1
        except Exception:
            pass
    return n


def host_classes():
    cls = {}
    for h, j in rows("SELECT h.str_name, j.str_name FROM proc p "
                     "JOIN host h ON h.pk_host=p.pk_host "
                     "JOIN job j ON j.pk_job=p.pk_job "
                     "WHERE j.str_name LIKE '%simflten%';"):
        cls[h] = "A" if "simfltena" in j else "B"
    return cls


def flood_procs():
    return [(h, int(cp), int(mem)) for h, cp, mem in
            rows("SELECT h.str_name, p.int_cores_reserved, p.int_mem_reserved "
                 "FROM proc p JOIN host h ON h.pk_host=p.pk_host "
                 "JOIN job j ON j.pk_job=p.pk_job "
                 "WHERE j.str_name LIKE '%simfltx%';")]


def flood_memmin():
    r = rows("SELECT l.int_mem_min FROM layer l JOIN job j ON j.pk_job=l.pk_job"
             " WHERE j.str_name LIKE '%simfltx%';")
    return int(r[0][0]) if r and r[0][0].strip().isdigit() else 0


def main():
    print(f"watching SQUEEZE_FLAT for {DURATION}s: a flat 50G layer vs "
          f"memory-short (tena) and cores-short (tenb) holes. Disease = "
          f">= {MIN_KILLS_DISEASE} cuebot kills of the flood; fixed = 0 kills,"
          f" cores-short squeezes still book, ask never ratchets.\n", flush=True)
    t0 = time.time()
    peak_running = 0
    peak_sq_b = 0
    sq_a_ever = 0
    memmin_final = 0
    memmin_ratcheted = False
    while time.time() - t0 < DURATION:
        t = time.time() - t0
        cls = host_classes()
        procs = flood_procs()
        running = len(procs)
        sq_a = sum(1 for h, cp, _ in procs if cp < ASK_CP and cls.get(h) == "A")
        sq_b = sum(1 for h, cp, _ in procs if cp < ASK_CP and cls.get(h) == "B")
        kills = kill_count()
        memmin = flood_memmin()
        if memmin > 0:
            memmin_final = memmin
            if memmin > ASK_MEM_KB:
                memmin_ratcheted = True
        peak_running = max(peak_running, running)
        peak_sq_b = max(peak_sq_b, sq_b)
        sq_a_ever = max(sq_a_ever, sq_a)
        print(f"t={t:5.0f} | fltx procs {running:3d} | squeezed A {sq_a} "
              f"B {sq_b} | kills {kills:3d} | ask {memmin // 1024}mb",
              flush=True)
        time.sleep(INTERVAL)

    kills = kill_count()
    print("\n==== SQUEEZE_FLAT VERDICT ====", flush=True)
    print(f"kills {kills}; squeezed-on-memory-short ever {sq_a_ever}; peak "
          f"squeezed-on-cores-short {peak_sq_b}; peak flood procs "
          f"{peak_running}; final ask {memmin_final // 1024}mb "
          f"(ratcheted: {memmin_ratcheted})", flush=True)
    if peak_running < 4:
        print(f"INCONCLUSIVE: only {peak_running} flood procs ever ran; the "
              f"workload never took hold.", flush=True)
    elif kills >= MIN_KILLS_DISEASE:
        print(f"FAIL (disease): {kills} cuebot memory kills -- the squeeze "
              f"under-reserved the flat 50G footprint into memory-short holes "
              f"and the host-OOM balancer entered the kill cycle"
              + (", then the layer ask ratcheted" if memmin_ratcheted else "")
              + ".", flush=True)
    elif kills == 0 and sq_a_ever == 0 and peak_sq_b >= 2 and not memmin_ratcheted:
        print(f"PASS: no kills, memory-short holes stayed un-squeezed, "
              f"cores-short holes booked {peak_sq_b} squeezed frames, and the "
              f"ask held at its declaration.", flush=True)
    else:
        print(f"FAIL: kills {kills}, memory-short squeezes {sq_a_ever}, "
              f"cores-short peak {peak_sq_b}, ratcheted {memmin_ratcheted} -- "
              f"not the disease, not the healthy contract either.", flush=True)


if __name__ == "__main__":
    main()
