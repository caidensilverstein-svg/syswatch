#!/usr/bin/env python3
"""
HELIX — Master backbone for SYSWATCH V5.
All signals flow through HELIX. Never referenced by name in the UI.

Internal systems:
  NUCLEO  — signal router, urgency 1-10
  PRISM   — multi-view metric decomposition
  ECHO    — session memory and pattern recognition
  DEOX    — noise cancellation / veil
  TBE     — truth baseline engine
  FLUX    — real-time state diff engine
  TRACE   — causal chain reconstruction + process genealogy
  ATLAS   — resource forecasting
  PULSE   — internal system heartbeat monitor
  SHARD   — metric compression and archival
  TTE     — dynamic threshold topology engine
  THETA   — time context engine
  SCRIBE  — annotation and memory layer
  MICRO   — shadow process monitor (adaptive-rate sampling)
  GRID    — per-instance event bus
  SRE     — system repair engine
"""

import os, time, uuid, sqlite3, threading, statistics, subprocess, json, hashlib
from collections import deque, defaultdict
from datetime import datetime

DB_PATH = os.path.expanduser("~/.syswatch_v5.db")

# ══════════════════════════════════════════════════════════════════════════════
# GRID — per-instance event bus (no cross-machine contamination)
# ══════════════════════════════════════════════════════════════════════════════
class GRID:
    def __init__(self):
        self._subs  = defaultdict(list)
        self._lock  = threading.Lock()
        self._queue = deque(maxlen=1000)
        self._worker = threading.Thread(target=self._drain, daemon=True)
        self._worker.start()

    def subscribe(self, event_type: str, callback):
        with self._lock:
            self._subs[event_type].append(callback)

    def publish(self, event_type: str, payload: dict):
        payload = dict(payload)
        payload["_ts"]   = time.time()
        payload["_type"] = event_type
        self._queue.append((event_type, payload))

    def _drain(self):
        while True:
            while self._queue:
                try:
                    event_type, payload = self._queue.popleft()
                except IndexError:
                    break
                with self._lock:
                    cbs = list(self._subs.get(event_type, []) +
                               self._subs.get("*", []))
                for cb in cbs:
                    try:
                        cb(payload)
                    except Exception:
                        pass
            time.sleep(0.01)

# ══════════════════════════════════════════════════════════════════════════════
# DB INIT
# ══════════════════════════════════════════════════════════════════════════════
_db_init_done = False
_db_init_lock = threading.Lock()

def init_db():
    global _db_init_done
    with _db_init_lock:
        if _db_init_done:
            return
        con = sqlite3.connect(DB_PATH)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.executescript("""
        CREATE TABLE IF NOT EXISTS annotations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            machine     TEXT,
            ts          REAL NOT NULL,
            label       TEXT NOT NULL,
            body        TEXT DEFAULT '',
            auto        INTEGER DEFAULT 0,
            urgency     INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_ann_machine ON annotations(machine, ts DESC);

        CREATE TABLE IF NOT EXISTS session_fingerprints (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            machine         TEXT NOT NULL,
            started_at      REAL NOT NULL,
            ended_at        REAL,
            tag             TEXT DEFAULT '',
            peak_cpu        REAL DEFAULT 0,
            peak_ram        REAL DEFAULT 0,
            incident_count  INTEGER DEFAULT 0,
            fp_hash         TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_sf_machine ON session_fingerprints(machine, started_at DESC);

        CREATE TABLE IF NOT EXISTS process_genealogy (
            machine         TEXT NOT NULL,
            proc_name       TEXT NOT NULL,
            first_seen      REAL NOT NULL,
            appearances     INTEGER DEFAULT 0,
            total_cpu       REAL DEFAULT 0,
            total_ram       REAL DEFAULT 0,
            incident_count  INTEGER DEFAULT 0,
            reputation      REAL DEFAULT 5.0,
            PRIMARY KEY (machine, proc_name)
        );

        CREATE TABLE IF NOT EXISTS metric_archive (
            machine     TEXT NOT NULL,
            metric      TEXT NOT NULL,
            ts          REAL NOT NULL,
            value       REAL NOT NULL,
            resolution  INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ma_lookup ON metric_archive(machine, metric, ts DESC);

        CREATE TABLE IF NOT EXISTS sre_actions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            machine     TEXT NOT NULL,
            ts          REAL NOT NULL,
            action_id   TEXT NOT NULL,
            trigger     TEXT DEFAULT '',
            command     TEXT NOT NULL,
            description TEXT DEFAULT '',
            executed    INTEGER DEFAULT 0,
            outcome     TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS tte_baselines (
            machine     TEXT NOT NULL,
            metric      TEXT NOT NULL,
            hour        INTEGER NOT NULL,
            n           INTEGER DEFAULT 0,
            mean        REAL DEFAULT 0,
            m2          REAL DEFAULT 0,
            p90_est     REAL DEFAULT 0,
            PRIMARY KEY (machine, metric, hour)
        );
        """)
        con.commit()
        con.close()
        _db_init_done = True

def get_db():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con

# ══════════════════════════════════════════════════════════════════════════════
# THETA — time context engine
# ══════════════════════════════════════════════════════════════════════════════
class THETA:
    def context(self) -> dict:
        now  = datetime.now()
        hour = now.hour
        return {
            "hour":        hour,
            "is_night":    0  <= hour < 6,
            "is_morning":  6  <= hour < 9,
            "is_business": 9  <= hour < 18,
            "is_evening":  18 <= hour < 23,
            "is_weekend":  now.weekday() >= 5,
            "time_str":    now.strftime("%H:%M"),
            "day_str":     now.strftime("%A"),
        }

    def urgency_modifier(self) -> float:
        ctx = self.context()
        if ctx["is_night"]:    return 1.3   # anomalies at 3am are more concerning
        if ctx["is_morning"]:  return 1.1
        if ctx["is_business"]: return 1.0
        return 0.9

# ══════════════════════════════════════════════════════════════════════════════
# PRISM — multi-view metric decomposition
# ══════════════════════════════════════════════════════════════════════════════
class PRISM:
    def __init__(self, window: int = 60):
        self._hist = deque(maxlen=window)

    def push(self, val: float):
        self._hist.append(val)

    def decompose(self, val: float) -> dict:
        h = list(self._hist)
        if len(h) < 2:
            return {"raw": val, "rate": 0.0, "accel": 0.0,
                    "z_score": 0.0, "percentile": 50.0, "mean": val, "std": 0.0}
        mean = statistics.mean(h)
        std  = statistics.stdev(h) if len(h) > 1 else 1.0
        z    = (val - mean) / max(std, 0.1)
        rate  = h[-1] - h[-2] if len(h) >= 2 else 0.0
        accel = ((h[-1] - h[-2]) - (h[-2] - h[-3])) if len(h) >= 3 else 0.0
        pct   = sum(1 for x in h if x <= val) / len(h) * 100
        return {
            "raw":        val,
            "mean":       round(mean, 1),
            "std":        round(std, 1),
            "rate":       round(rate, 2),
            "accel":      round(accel, 2),
            "z_score":    round(z, 2),
            "percentile": round(pct, 1),
        }

# ══════════════════════════════════════════════════════════════════════════════
# TTE — dynamic threshold topology engine
#   Uses online Welford mean/variance per (machine, metric, hour).
#   p90 estimated as mean + 1.28*std (normal approximation).
#   Falls back to hardcoded defaults until 20+ samples accumulated.
# ══════════════════════════════════════════════════════════════════════════════
class TTE:
    DEFAULTS = {"cpu": 75.0, "ram": 80.0, "temp": 85.0}

    def __init__(self, machine: str):
        self.machine  = machine
        self._cache   = {}   # (metric, hour) -> {n, mean, m2}
        self._dirty   = set()
        self._load()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def _load(self):
        try:
            con = get_db()
            rows = con.execute(
                "SELECT metric, hour, n, mean, m2, p90_est FROM tte_baselines WHERE machine=?",
                (self.machine,)
            ).fetchall()
            con.close()
            for r in rows:
                self._cache[(r["metric"], r["hour"])] = {
                    "n": r["n"], "mean": r["mean"], "m2": r["m2"], "p90": r["p90_est"]
                }
        except Exception:
            pass

    def record(self, metric: str, val: float):
        hour = datetime.now().hour
        key  = (metric, hour)
        if key not in self._cache:
            self._cache[key] = {"n": 0, "mean": 0.0, "m2": 0.0, "p90": self.DEFAULTS.get(metric, 80.0)}
        c = self._cache[key]
        c["n"]   += 1
        delta     = val - c["mean"]
        c["mean"] += delta / c["n"]
        c["m2"]  += delta * (val - c["mean"])
        if c["n"] >= 2:
            std     = (c["m2"] / (c["n"] - 1)) ** 0.5
            c["p90"] = c["mean"] + 1.28 * std
        self._dirty.add(key)

    def get_threshold(self, metric: str) -> float:
        hour = datetime.now().hour
        c    = self._cache.get((metric, hour))
        if c and c["n"] >= 20:
            return c["p90"]
        return self.DEFAULTS.get(metric, 80.0)

    def _flush_loop(self):
        while True:
            time.sleep(60)
            if not self._dirty:
                continue
            try:
                con  = get_db()
                keys = list(self._dirty)
                self._dirty.clear()
                for key in keys:
                    c = self._cache.get(key)
                    if not c:
                        continue
                    metric, hour = key
                    con.execute("""
                        INSERT INTO tte_baselines (machine,metric,hour,n,mean,m2,p90_est)
                        VALUES (?,?,?,?,?,?,?)
                        ON CONFLICT(machine,metric,hour) DO UPDATE SET
                            n=excluded.n, mean=excluded.mean,
                            m2=excluded.m2, p90_est=excluded.p90_est
                    """, (self.machine, metric, hour,
                          c["n"], c["mean"], c["m2"], c["p90"]))
                con.commit()
                con.close()
            except Exception:
                pass

# ══════════════════════════════════════════════════════════════════════════════
# DEOX — noise cancellation / veil
# ══════════════════════════════════════════════════════════════════════════════
class DEOX:
    def __init__(self):
        # Welford online stats per metric for veil
        self._veil      = {}   # metric -> {mean, m2, n}
        self._suppressed = {}  # (metric, proc) -> suppress_until_ts
        self._heal_log  = defaultdict(list)  # (metric, proc) -> [ts, ...]

    def update_veil(self, metric: str, val: float):
        if metric not in self._veil:
            self._veil[metric] = {"mean": val, "m2": 0.0, "n": 1}
            return
        v = self._veil[metric]
        v["n"]   += 1
        delta     = val - v["mean"]
        v["mean"] += delta / v["n"]
        v["m2"]  += delta * (val - v["mean"])

    def is_veiled(self, metric: str, val: float) -> bool:
        v = self._veil.get(metric)
        if not v or v["n"] < 30:
            return False
        std = (v["m2"] / max(1, v["n"] - 1)) ** 0.5
        # Within 1.5 std of mean → normal, veil it
        return abs(val - v["mean"]) < max(std * 1.5, 3.0)

    def record_self_heal(self, metric: str, proc: str):
        """Call when an anomaly for (metric, proc) resolves on its own."""
        key  = (metric, proc)
        now  = time.time()
        self._heal_log[key].append(now)
        # Trim to last hour
        self._heal_log[key] = [t for t in self._heal_log[key] if now - t < 3600]
        # Suppress if self-healed 5+ times in the last hour
        if len(self._heal_log[key]) >= 5:
            self._suppressed[key] = now + 1800  # suppress for 30 min

    def is_suppressed(self, metric: str, proc: str) -> bool:
        return time.time() < self._suppressed.get((metric, proc), 0)

    def heal_count(self, metric: str, proc: str) -> int:
        return len(self._heal_log.get((metric, proc), []))

# ══════════════════════════════════════════════════════════════════════════════
# TBE — truth baseline engine
#   Tracks irreducible service floors. Generic, no hard-coded service names.
# ══════════════════════════════════════════════════════════════════════════════
class TBE:
    SERVICE_FLOORS = {
        "ram_headroom_min_pct": 15.0,
        "cpu_headroom_min_pct": 10.0,
    }

    def __init__(self, machine: str):
        self.machine = machine

    def threatens_floor(self, cpu_pct: float, ram_pct: float) -> list:
        threats = []
        cpu_headroom = 100.0 - cpu_pct
        ram_headroom = 100.0 - ram_pct
        if cpu_headroom < self.SERVICE_FLOORS["cpu_headroom_min_pct"]:
            threats.append(
                f"CPU headroom critically low: {cpu_headroom:.1f}% remaining"
            )
        if ram_headroom < self.SERVICE_FLOORS["ram_headroom_min_pct"]:
            threats.append(
                f"RAM headroom critically low: {ram_headroom:.1f}% remaining"
            )
        return threats

# ══════════════════════════════════════════════════════════════════════════════
# ATLAS — resource forecasting (OLS linear regression)
# ══════════════════════════════════════════════════════════════════════════════
class ATLAS:
    def __init__(self, window: int = 120):
        self._series  = {}
        self._window  = window
        self._cache   = {}   # metric -> (computed_at, result)
        self._cache_ttl = 15 # seconds

    def push(self, metric: str, val: float):
        if metric not in self._series:
            self._series[metric] = deque(maxlen=self._window)
        self._series[metric].append((time.time(), val))
        # Invalidate cache
        self._cache.pop(metric, None)

    def forecast(self, metric: str, horizon_s: int = 300) -> dict:
        # Return cached result if fresh
        cached = self._cache.get(metric)
        if cached and time.time() - cached[0] < self._cache_ttl:
            return cached[1]

        series = list(self._series.get(metric, []))
        if len(series) < 15:
            return {"available": False}

        t0 = series[0][0]
        xs = [s[0] - t0 for s in series]
        ys = [s[1]       for s in series]
        n  = len(xs)
        sx  = sum(xs);  sy  = sum(ys)
        sxx = sum(x*x for x in xs)
        sxy = sum(x*y for x, y in zip(xs, ys))
        denom = n * sxx - sx * sx

        if abs(denom) < 1e-9:
            return {"available": False}

        slope     = (n * sxy - sx * sy) / denom
        intercept = (sy - slope * sx) / n
        predicted = intercept + slope * (xs[-1] + horizon_s)
        predicted = max(0.0, min(100.0, predicted))

        # R² for confidence
        y_mean = sy / n
        ss_tot = sum((y - y_mean) ** 2 for y in ys)
        y_hat  = [intercept + slope * x for x in xs]
        ss_res = sum((y - yh) ** 2 for y, yh in zip(ys, y_hat))
        r2     = max(0.0, 1.0 - ss_res / max(ss_tot, 1e-9))

        minutes_to_90 = None
        if slope > 1e-4 and ys[-1] < 90:
            t90   = (90.0 - intercept) / slope
            delta = t90 - xs[-1]
            if 0 < delta < 7200:
                minutes_to_90 = round(delta / 60, 1)

        result = {
            "available":      True,
            "current":        round(ys[-1], 1),
            "predicted":      round(predicted, 1),
            "slope_per_min":  round(slope * 60, 3),
            "confidence":     round(r2, 2),
            "minutes_to_90":  minutes_to_90,
            "horizon_s":      horizon_s,
            "direction":      ("rising"  if slope >  0.05 / 60 else
                               "falling" if slope < -0.05 / 60 else "stable"),
        }
        self._cache[metric] = (time.time(), result)
        return result

# ══════════════════════════════════════════════════════════════════════════════
# FLUX — real-time state diff engine
# ══════════════════════════════════════════════════════════════════════════════
class FLUX:
    def __init__(self):
        self._prev      = {}
        self._history   = deque(maxlen=120)
        self._lock      = threading.Lock()

    def _snap(self, state: dict) -> dict:
        return {
            "cpu":        round(state.get("cpu_pct", 0), 1),
            "ram":        round(state.get("ram_pct", 0), 1),
            "procs":      frozenset(n for n, _ in state.get("cpu_top", [])),
            "net_count":  len(state.get("net_connections", [])),
            "pm2":        {p["name"]: p["status"]
                           for p in state.get("pm2_processes", [])},
        }

    def diff(self, machine: str, state: dict) -> list:
        snap = self._snap(state)
        with self._lock:
            prev = self._prev.get(machine, {})
            self._prev[machine] = snap

        if not prev:
            return []

        changes = []
        cpu_d = snap["cpu"] - prev.get("cpu", snap["cpu"])
        ram_d = snap["ram"] - prev.get("ram", snap["ram"])
        if abs(cpu_d) >= 5:
            changes.append({"type": "cpu_delta", "delta": cpu_d,
                             "desc": f"CPU {'▲' if cpu_d>0 else '▼'}{abs(cpu_d):.1f}%"})
        if abs(ram_d) >= 3:
            changes.append({"type": "ram_delta", "delta": ram_d,
                             "desc": f"RAM {'▲' if ram_d>0 else '▼'}{abs(ram_d):.1f}%"})

        new_p  = snap["procs"] - prev.get("procs", frozenset())
        gone_p = prev.get("procs", frozenset()) - snap["procs"]
        for p in new_p:
            changes.append({"type": "new_proc",  "proc": p, "desc": f"New process: {p}"})
        for p in gone_p:
            changes.append({"type": "gone_proc", "proc": p, "desc": f"Process ended: {p}"})

        for name, status in snap["pm2"].items():
            prev_status = prev.get("pm2", {}).get(name)
            if prev_status and prev_status != status:
                changes.append({"type": "pm2_change", "proc": name,
                                 "desc": f"Service {name}: {prev_status} → {status}"})

        if changes:
            with self._lock:
                self._history.append({"ts": time.time(), "machine": machine, "changes": changes})
        return changes

    def get_recent(self, n: int = 10) -> list:
        with self._lock:
            return list(self._history)[-n:]

# ══════════════════════════════════════════════════════════════════════════════
# TRACE — causal chain reconstruction + process genealogy
# ══════════════════════════════════════════════════════════════════════════════
class TRACE:
    def __init__(self, machine: str):
        self.machine    = machine
        self._causal    = deque(maxlen=1000)
        self._genealogy = {}
        self._dirty     = set()
        self._lock      = threading.Lock()
        self._load()
        threading.Thread(target=self._flush_loop, daemon=True).start()

    def _load(self):
        try:
            con  = get_db()
            rows = con.execute(
                "SELECT * FROM process_genealogy WHERE machine=?", (self.machine,)
            ).fetchall()
            con.close()
            with self._lock:
                for r in rows:
                    self._genealogy[r["proc_name"]] = dict(r)
        except Exception:
            pass

    def record_proc(self, proc: str, cpu: float, ram: float):
        with self._lock:
            now = time.time()
            if proc not in self._genealogy:
                self._genealogy[proc] = {
                    "proc_name": proc, "machine": self.machine,
                    "first_seen": now, "appearances": 0,
                    "total_cpu": 0.0, "total_ram": 0.0,
                    "incident_count": 0, "reputation": 5.0,
                }
            g = self._genealogy[proc]
            g["appearances"] += 1
            g["total_cpu"]   += cpu
            g["total_ram"]   += ram
            if g["appearances"] % 120 == 0:
                self._dirty.add(proc)

    def record_event(self, event_type: str, proc: str, val: float):
        self._causal.append({
            "ts": time.time(), "type": event_type,
            "proc": proc, "val": val
        })

    def mark_incident(self, proc: str):
        with self._lock:
            if proc in self._genealogy:
                self._genealogy[proc]["incident_count"] += 1
                self._genealogy[proc]["reputation"] = max(
                    1.0, self._genealogy[proc]["reputation"] - 0.5
                )
                self._dirty.add(proc)

    def reconstruct_chain(self, incident_ts: float, window_s: float = 300) -> list:
        return sorted(
            [e for e in self._causal
             if incident_ts - window_s <= e["ts"] <= incident_ts],
            key=lambda x: x["ts"]
        )

    def get_biography(self, proc: str) -> dict:
        with self._lock:
            g = self._genealogy.get(proc)
        if not g:
            return {}
        n = max(1, g["appearances"])
        return {
            "proc":        proc,
            "first_seen":  g["first_seen"],
            "appearances": g["appearances"],
            "avg_cpu":     round(g["total_cpu"] / n, 1),
            "avg_ram":     round(g["total_ram"] / n, 1),
            "reputation":  round(g["reputation"], 1),
            "incidents":   g["incident_count"],
            "known":       g["appearances"] > 50,
        }

    def get_new_procs(self) -> list:
        with self._lock:
            return [p for p, g in self._genealogy.items() if g["appearances"] < 5]

    def _flush_loop(self):
        while True:
            time.sleep(60)
            if not self._dirty:
                continue
            try:
                with self._lock:
                    procs = list(self._dirty)
                    self._dirty.clear()
                    rows  = [self._genealogy[p] for p in procs if p in self._genealogy]
                con = get_db()
                con.executemany("""
                    INSERT INTO process_genealogy
                    (machine,proc_name,first_seen,appearances,total_cpu,total_ram,incident_count,reputation)
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT(machine,proc_name) DO UPDATE SET
                        appearances=excluded.appearances,
                        total_cpu=excluded.total_cpu,
                        total_ram=excluded.total_ram,
                        incident_count=excluded.incident_count,
                        reputation=excluded.reputation
                """, [
                    (g["machine"], g["proc_name"], g["first_seen"],
                     g["appearances"], g["total_cpu"], g["total_ram"],
                     g["incident_count"], g["reputation"])
                    for g in rows
                ])
                con.commit()
                con.close()
            except Exception:
                pass

# ══════════════════════════════════════════════════════════════════════════════
# ECHO — session memory and pattern recognition
#   Fingerprint = hash of (peak_cpu_bucket, peak_ram_bucket,
#                          incident_count_bucket, top_proc_set_hash)
#   Matching: fingerprint similarity + metric proximity
#   Runbook: write after 3 identical fp_hash matches
# ══════════════════════════════════════════════════════════════════════════════
class ECHO:
    def __init__(self, machine: str):
        self.machine         = machine
        self._session_start  = time.time()
        self._session_id     = None
        self._incident_count = 0
        self._top_procs      = []
        self._runbooks       = {}  # fp_hash -> runbook text
        self._init_session()
        self._load_runbooks()

    def _init_session(self):
        try:
            con = get_db()
            cur = con.execute(
                "INSERT INTO session_fingerprints (machine, started_at) VALUES (?,?)",
                (self.machine, self._session_start)
            )
            self._session_id = cur.lastrowid
            con.commit()
            con.close()
        except Exception:
            self._session_id = None

    def _load_runbooks(self):
        """Load runbook annotations from previous sessions."""
        try:
            con  = get_db()
            rows = con.execute("""
                SELECT body FROM annotations
                WHERE machine=? AND label LIKE 'RUNBOOK:%' AND auto=1
                ORDER BY ts DESC LIMIT 20
            """, (self.machine,)).fetchall()
            con.close()
            for r in rows:
                try:
                    rb = json.loads(r["body"])
                    self._runbooks[rb["fp_hash"]] = rb["text"]
                except Exception:
                    pass
        except Exception:
            pass

    def record_incident(self):
        self._incident_count += 1

    def update_top_procs(self, cpu_top: list):
        self._top_procs = [n for n, _ in cpu_top[:5]]

    def _build_fingerprint(self, peak_cpu: float, peak_ram: float) -> str:
        cpu_bucket  = int(peak_cpu  // 10) * 10
        ram_bucket  = int(peak_ram  // 10) * 10
        inc_bucket  = min(self._incident_count // 2, 5)
        proc_hash   = hashlib.md5(
            ",".join(sorted(self._top_procs)).encode()
        ).hexdigest()[:8]
        raw = f"{cpu_bucket}:{ram_bucket}:{inc_bucket}:{proc_hash}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def match_past_sessions(self, peak_cpu: float, peak_ram: float) -> list:
        fp = self._build_fingerprint(peak_cpu, peak_ram)
        try:
            con  = get_db()
            rows = con.execute("""
                SELECT tag, started_at, peak_cpu, peak_ram, incident_count, fp_hash
                FROM session_fingerprints
                WHERE machine=? AND ended_at IS NOT NULL
                ORDER BY started_at DESC LIMIT 50
            """, (self.machine,)).fetchall()
            con.close()
        except Exception:
            return []

        matches = []
        for r in rows:
            score = 0
            # Exact fingerprint match = 3 pts
            if r["fp_hash"] == fp:
                score += 3
            # Metric proximity
            if r["peak_cpu"] and abs(r["peak_cpu"] - peak_cpu) < 15:
                score += 1
            if r["peak_ram"] and abs(r["peak_ram"] - peak_ram) < 15:
                score += 1
            if score >= 2:
                matches.append({"score": score, **dict(r)})

        matches.sort(key=lambda x: x["score"], reverse=True)

        # Check if runbook should be written (3+ exact matches)
        exact = [m for m in matches if m.get("fp_hash") == fp]
        if len(exact) >= 3 and fp not in self._runbooks:
            self._write_runbook(fp, exact)

        return matches[:3]

    def _write_runbook(self, fp: str, sessions: list):
        tags   = [s.get("tag", "") for s in sessions if s.get("tag")]
        text   = f"Recurring pattern ({len(sessions)} occurrences). Common tags: {', '.join(set(tags))}."
        rb     = {"fp_hash": fp, "text": text}
        self._runbooks[fp] = text
        try:
            con = get_db()
            con.execute(
                "INSERT INTO annotations (machine,ts,label,body,auto,urgency) VALUES (?,?,?,?,?,?)",
                (self.machine, time.time(), f"RUNBOOK:{fp}", json.dumps(rb), 1, 4)
            )
            con.commit()
            con.close()
        except Exception:
            pass

    def get_runbook(self, peak_cpu: float, peak_ram: float) -> str:
        fp = self._build_fingerprint(peak_cpu, peak_ram)
        return self._runbooks.get(fp, "")

    def auto_tag_session(self, peak_cpu: float, peak_ram: float,
                          service_restarts: int) -> str:
        tags = []
        if peak_cpu > 80:            tags.append("heavy CPU")
        if peak_ram > 85:            tags.append("RAM pressure")
        if service_restarts > 0:     tags.append(f"service instability ({service_restarts} restarts)")
        if self._incident_count > 5: tags.append("incident-heavy")
        if not tags:                  tags.append("nominal")
        return ", ".join(tags)

    def flush(self, peak_cpu: float, peak_ram: float, tag: str):
        fp = self._build_fingerprint(peak_cpu, peak_ram)
        if self._session_id is None:
            return
        try:
            con = get_db()
            con.execute("""
                UPDATE session_fingerprints
                SET ended_at=?, peak_cpu=?, peak_ram=?, incident_count=?, tag=?, fp_hash=?
                WHERE id=?
            """, (time.time(), peak_cpu, peak_ram,
                  self._incident_count, tag, fp, self._session_id))
            con.commit()
            con.close()
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════════════════════
# SCRIBE — annotation and memory layer
# ══════════════════════════════════════════════════════════════════════════════
class SCRIBE:
    def __init__(self, grid: "GRID"):
        self._grid       = grid
        self._cache      = deque(maxlen=200)
        self._lock       = threading.Lock()
        self._write_buf  = []
        self._buf_lock   = threading.Lock()
        self._load_recent()
        threading.Thread(target=self._flush_loop, daemon=True).start()

    def _load_recent(self):
        try:
            con  = get_db()
            rows = con.execute(
                "SELECT * FROM annotations ORDER BY ts ASC LIMIT 200"
            ).fetchall()
            con.close()
            with self._lock:
                self._cache.clear()
                for r in rows:
                    self._cache.append(dict(r))
        except Exception:
            pass

    def write(self, machine: str, label: str, body: str = "",
              auto: bool = False, urgency: int = 0) -> dict:
        ts    = time.time()
        entry = {"machine": machine, "ts": ts, "label": label,
                 "body": body, "auto": int(auto), "urgency": urgency}
        with self._lock:
            self._cache.append(entry)
        with self._buf_lock:
            self._write_buf.append(entry)
        self._grid.publish("annotation", entry)
        return entry

    def _flush_loop(self):
        while True:
            time.sleep(5)
            with self._buf_lock:
                buf = self._write_buf[:]
                self._write_buf.clear()
            if not buf:
                continue
            try:
                con = get_db()
                con.executemany(
                    "INSERT INTO annotations (machine,ts,label,body,auto,urgency) VALUES (?,?,?,?,?,?)",
                    [(e["machine"], e["ts"], e["label"],
                      e["body"], e["auto"], e["urgency"]) for e in buf]
                )
                con.commit()
                con.close()
            except Exception:
                pass

    def get_all(self, machine: str = None, limit: int = 50) -> list:
        with self._lock:
            items = list(self._cache)
        if machine:
            items = [a for a in items if a.get("machine") == machine]
        return list(reversed(items))[:limit]

# ══════════════════════════════════════════════════════════════════════════════
# MICRO — shadow process monitor (adaptive-rate sampling)
#   100ms when urgency ≥ 7 active, 500ms otherwise.
#   Catches flash processes that live < 1s.
# ══════════════════════════════════════════════════════════════════════════════
class MICRO:
    def __init__(self, machine: str, trace: TRACE, grid: "GRID"):
        self.machine      = machine
        self._trace       = trace
        self._grid        = grid
        self._seen        = set()
        self._flash_procs = deque(maxlen=100)
        self._high_freq   = False   # set True by HELIX when urgency ≥ 7
        self._running     = False

    def set_high_freq(self, val: bool):
        self._high_freq = val

    def start(self):
        if self._running:
            return
        self._running = True
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        try:
            import psutil
        except ImportError:
            return
        while self._running:
            try:
                current = set()
                for p in psutil.process_iter(["name"]):
                    try:
                        current.add(p.info["name"])
                    except Exception:
                        pass
                new_procs  = current - self._seen
                gone_procs = self._seen - current
                for name in gone_procs:
                    flash = {"proc": name, "ts": time.time(), "machine": self.machine}
                    self._flash_procs.append(flash)
                    self._grid.publish("flash_proc", flash)
                self._seen = current
            except Exception:
                pass
            # Adaptive sleep
            time.sleep(0.1 if self._high_freq else 0.5)

    def get_recent_flashes(self, limit: int = 10) -> list:
        return list(self._flash_procs)[-limit:]

# ══════════════════════════════════════════════════════════════════════════════
# SHARD — metric compression and archival (batched writes)
# ══════════════════════════════════════════════════════════════════════════════
class SHARD:
    # (max_age_s, resolution_s) — age is relative to now
    TIERS = [(600, 1), (3600, 10), (86400, 60), (None, 300)]
    FLUSH_INTERVAL = 30  # flush every 30 seconds, not every N ticks

    def __init__(self):
        self._buf      = []
        self._lock     = threading.Lock()
        self._last_flush = time.time()
        threading.Thread(target=self._flush_loop, daemon=True).start()

    def record(self, machine: str, metric: str, val: float):
        with self._lock:
            self._buf.append((machine, metric, time.time(), val))

    def _resolve(self, ts: float, now: float) -> int:
        age = now - ts
        for max_age, res in self.TIERS:
            if max_age is None or age < max_age:
                return res
        return 300

    def _flush_loop(self):
        while True:
            time.sleep(self.FLUSH_INTERVAL)
            with self._lock:
                buf = self._buf[:]
                self._buf.clear()
            if not buf:
                continue
            try:
                now  = time.time()
                rows = [(m, mt, ts, v, self._resolve(ts, now))
                        for m, mt, ts, v in buf]
                con  = get_db()
                con.executemany(
                    "INSERT INTO metric_archive (machine,metric,ts,value,resolution) VALUES (?,?,?,?,?)",
                    rows
                )
                con.commit()
                con.close()
            except Exception:
                pass

    def get_history(self, machine: str, metric: str,
                    since: float = None, limit: int = 500) -> list:
        since = since or (time.time() - 3600)
        try:
            con  = get_db()
            rows = con.execute("""
                SELECT ts, value FROM metric_archive
                WHERE machine=? AND metric=? AND ts>?
                ORDER BY ts ASC LIMIT ?
            """, (machine, metric, since, limit)).fetchall()
            con.close()
            return [{"ts": r["ts"], "value": r["value"]} for r in rows]
        except Exception:
            return []

# ══════════════════════════════════════════════════════════════════════════════
# SRE — system repair engine
#   Uses uuid4 for action IDs (no collision on same-second proposals).
#   Playbook is generic — no hard-coded service names.
# ══════════════════════════════════════════════════════════════════════════════
class SRE:
    PLAYBOOK = {
        "high_cpu": [
            {"cmd": "ps aux --sort=-%cpu | head -15",
             "desc": "Show top CPU consumers"},
        ],
        "high_ram": [
            {"cmd": "free -h && ps aux --sort=-%mem | head -10",
             "desc": "Show memory usage and top RAM consumers"},
        ],
        "service_down": [
            {"cmd": "pm2 status",
             "desc": "Check all service statuses"},
            {"cmd": "pm2 restart {proc}",
             "desc": "Restart affected service"},
        ],
        "disk_pressure": [
            {"cmd": "df -h",
             "desc": "Show disk usage"},
            {"cmd": "du -sh /var/log/* 2>/dev/null | sort -rh | head -10",
             "desc": "Find large log files"},
        ],
    }

    def __init__(self, machine: str):
        self.machine  = machine
        self._pending = {}   # action_id -> action dict
        self._history = deque(maxlen=100)

    def propose(self, trigger: str, context: dict) -> list:
        key = None
        if context.get("cpu_pct", 0) > 85:    key = "high_cpu"
        if context.get("ram_pct", 0) > 85:    key = "high_ram"
        if trigger == "service_down":          key = "service_down"
        if trigger == "disk_pressure":         key = "disk_pressure"
        if not key:
            return []

        actions = []
        for play in self.PLAYBOOK.get(key, []):
            if play.get("dangerous") or play.get("root"):
                continue
            try:
                cmd = play["cmd"].format(**context)
            except KeyError:
                cmd = play["cmd"]
            action = {
                "id":          str(uuid.uuid4()),
                "cmd":         cmd,
                "desc":        play["desc"],
                "trigger":     trigger,
                "proposed_at": time.time(),
                "machine":     self.machine,
                "executed":    False,
                "outcome":     "",
            }
            self._pending[action["id"]] = action
            actions.append(action)
        return actions

    def execute(self, action_id: str, remote_fn=None) -> dict:
        action = self._pending.get(action_id)
        if not action:
            return {"ok": False, "error": "action not found"}
        try:
            if remote_fn:
                output = remote_fn(action["cmd"])
            else:
                result = subprocess.run(
                    action["cmd"], shell=True,
                    capture_output=True, text=True, timeout=15
                )
                output = result.stdout + result.stderr
            action["executed"]    = True
            action["outcome"]     = str(output)[:500]
            action["executed_at"] = time.time()
            self._history.append(action)
            del self._pending[action_id]
            try:
                con = get_db()
                con.execute("""
                    INSERT INTO sre_actions
                    (machine,ts,action_id,trigger,command,description,executed,outcome)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (self.machine, time.time(), action_id, action["trigger"],
                      action["cmd"], action["desc"], 1, action["outcome"]))
                con.commit()
                con.close()
            except Exception:
                pass
            return {"ok": True, "output": action["outcome"]}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_pending(self) -> list:
        return list(self._pending.values())

    def get_history(self) -> list:
        return list(self._history)

# ══════════════════════════════════════════════════════════════════════════════
# PULSE — internal system heartbeat
# ══════════════════════════════════════════════════════════════════════════════
class PULSE:
    def __init__(self):
        self._beats = {}
        self._lock  = threading.Lock()

    def beat(self, system: str):
        with self._lock:
            self._beats[system] = time.time()

    def check(self) -> dict:
        now = time.time()
        with self._lock:
            beats = dict(self._beats)
        report = {}
        for sys, ts in beats.items():
            age = now - ts
            report[sys] = {
                "alive":  age < 10,
                "age_s":  round(age, 1),
                "status": "alive" if age < 10 else "stale" if age < 30 else "dead",
            }
        return report

# ══════════════════════════════════════════════════════════════════════════════
# VIE — visual intelligence engine
# ══════════════════════════════════════════════════════════════════════════════
class VIE:
    def __init__(self):
        self._urgency  = 0
        self._history  = deque(maxlen=60)

    def update(self, urgency: int) -> dict:
        self._urgency = urgency
        self._history.append({"ts": time.time(), "urgency": urgency})
        if urgency >= 9:
            return {"alert_level": "critical", "expand": True,  "dim_other": True}
        if urgency >= 7:
            return {"alert_level": "urgent",   "expand": False, "dim_other": False}
        if urgency >= 5:
            return {"alert_level": "elevated",  "expand": False, "dim_other": False}
        return {"alert_level": "normal", "expand": False, "dim_other": False}

    def get_state(self) -> dict:
        return {
            "current_urgency": self._urgency,
            "history": list(self._history)[-20:],
        }

# ══════════════════════════════════════════════════════════════════════════════
# NUCLEO — signal router, urgency 1-10
# ══════════════════════════════════════════════════════════════════════════════
ROUTING_DELAYS = {
    "deferred": 8.0,
    "normal":   2.0,
    "elevated": 0.5,
    "urgent":   0.0,
    "critical": 0.0,
}

def _urgency_to_tier(u: int) -> str:
    if u <= 2: return "deferred"
    if u <= 4: return "normal"
    if u <= 6: return "elevated"
    if u <= 8: return "urgent"
    return "critical"

class NUCLEO:
    def __init__(self, deox: DEOX, tte: TTE, tbe: TBE,
                 prism_cpu: PRISM, prism_ram: PRISM,
                 theta: THETA, grid: "GRID"):
        self._deox      = deox
        self._tte       = tte
        self._tbe       = tbe
        self._prism_cpu = prism_cpu
        self._prism_ram = prism_ram
        self._theta     = theta
        self._grid      = grid
        # Lock-free queue: list of (Signal, queued_at)
        self._queue     = deque(maxlen=1000)
        self._queue_lock = threading.Lock()
        threading.Thread(target=self._route_loop, daemon=True).start()

    def score(self, sig_type: str, val: float = 0,
              metric: str = "", proc: str = "",
              context: dict = None) -> int:
        ctx  = context or {}
        base = 1

        if sig_type == "metric_update":
            prism  = self._prism_cpu if metric == "cpu" else self._prism_ram
            decomp = prism.decompose(val)
            z      = abs(decomp.get("z_score", 0.0))
            base   = min(10, max(1, int(z * 2.5)))
            if decomp.get("accel", 0) > 2:            base = min(10, base + 1)
            if val > self._tte.get_threshold(metric):  base = min(10, base + 1)
            if self._deox.is_veiled(metric, val):      base = max(1, base - 2)
            if self._deox.is_suppressed(metric, proc): base = min(4, base)

        elif sig_type == "pm2_crash":         base = 9
        elif sig_type == "pm2_restart":       base = 7
        elif sig_type == "anomaly_start":
            base = min(10, max(5, int(ctx.get("multiplier", 1.0) * 2.5)))
        elif sig_type == "anomaly_resolved":  base = 2
        elif sig_type == "threshold_breach":  base = 8
        elif sig_type == "tbe_floor_threat":  base = 10
        elif sig_type == "new_proc":          base = 6
        elif sig_type == "flash_proc":        base = 3
        elif sig_type == "atlas_breach":
            mins = ctx.get("minutes_to_90", 60)
            base = max(4, min(9, int(12 - mins / 8)))
        elif sig_type == "sre_proposal":      base = 7
        elif sig_type == "sre_executed":      base = 5
        elif sig_type == "flux_diff":
            base = 7 if ctx.get("diff_type") == "pm2_change" else 4
        else:
            base = 1

        # THETA modifier
        score = min(10, max(1, round(base * self._theta.urgency_modifier())))

        # TBE override — floor threat always ≥ 9
        if self._tbe.threatens_floor(ctx.get("cpu_pct", 0), ctx.get("ram_pct", 0)):
            score = max(score, 9)

        return score

    def submit(self, sig_type: str, val: float = 0,
               metric: str = "", proc: str = "",
               payload: dict = None, context: dict = None) -> int:
        urgency = self.score(sig_type, val, metric, proc, context)
        tier    = _urgency_to_tier(urgency)
        sig     = {
            "type":    sig_type, "val":     val,
            "metric":  metric,   "proc":    proc,
            "urgency": urgency,  "tier":    tier,
            "payload": payload or {},
        }
        with self._queue_lock:
            self._queue.append((sig, time.time()))
        self._grid.publish("signal", sig)
        return urgency

    def _route_loop(self):
        while True:
            now   = time.time()
            ready = []
            with self._queue_lock:
                remaining = deque(maxlen=1000)
                while self._queue:
                    sig, queued_at = self._queue.popleft()
                    if now - queued_at >= ROUTING_DELAYS[sig["tier"]]:
                        ready.append(sig)
                    else:
                        remaining.append((sig, queued_at))
                self._queue = remaining
            for sig in ready:
                self._grid.publish(f"routed_{sig['tier']}", sig)
            time.sleep(0.05)

    def get_queue_depth(self) -> dict:
        with self._queue_lock:
            tiers = defaultdict(int)
            for sig, _ in self._queue:
                tiers[sig["tier"]] += 1
        return dict(tiers)

# ══════════════════════════════════════════════════════════════════════════════
# HELIX — master controller
# ══════════════════════════════════════════════════════════════════════════════
class HELIX:
    def __init__(self, machine: str):
        self.machine = machine
        init_db()

        # Core systems
        self.grid      = GRID()
        self.theta     = THETA()
        self.prism_cpu = PRISM()
        self.prism_ram = PRISM()
        self.deox      = DEOX()
        self.tte       = TTE(machine)
        self.tbe       = TBE(machine)
        self.atlas     = ATLAS()
        self.flux      = FLUX()
        self.trace     = TRACE(machine)
        self.echo      = ECHO(machine)
        self.scribe    = SCRIBE(self.grid)
        self.sre       = SRE(machine)
        self.shard     = SHARD()
        self.pulse     = PULSE()
        self.micro     = MICRO(machine, self.trace, self.grid)
        self.vie       = VIE()
        self.nucleo    = NUCLEO(
            self.deox, self.tte, self.tbe,
            self.prism_cpu, self.prism_ram,
            self.theta, self.grid
        )

        self._priority_feed  = deque(maxlen=50)
        self._ai_forced      = False
        self._last_urgency   = 0
        self._forecasts      = {}
        self._vie_state      = {}
        self._anomaly_active = set()  # (proc, metric) currently spiking

        # Subscribe to routed events
        self.grid.subscribe("routed_critical", self._on_critical)
        self.grid.subscribe("routed_urgent",   self._on_urgent)
        self.grid.subscribe("sre_proposal",    self._on_sre_proposal)
        self.grid.subscribe("flash_proc",      self._on_flash_proc)
        self.grid.subscribe("signal",          self._on_signal)

        self.micro.start()
        threading.Thread(target=self._forecast_loop, daemon=True).start()
        threading.Thread(target=self._pulse_loop,    daemon=True).start()

    # ── Event handlers ────────────────────────────────────────────────────────
    def _on_signal(self, e):
        u = e.get("urgency", 0)
        if u > self._last_urgency:
            self._last_urgency = u
        self._vie_state = self.vie.update(u)
        self.micro.set_high_freq(u >= 7)

    def _on_critical(self, e):
        self._ai_forced = True
        label = f"[{e.get('type','?').upper()}] {e.get('metric','')} {e.get('proc','')}".strip()
        self.scribe.write(self.machine, label, auto=True, urgency=10)
        self._priority_feed.appendleft({
            "ts": time.time(), "urgency": 10,
            "label": label, "tier": "critical"
        })

    def _on_urgent(self, e):
        self._ai_forced = True
        label = f"[{e.get('type','?').upper()}] {e.get('metric','')} {e.get('proc','')}".strip()
        self._priority_feed.appendleft({
            "ts": time.time(), "urgency": e.get("urgency", 8),
            "label": label, "tier": "urgent"
        })

    def _on_sre_proposal(self, e):
        n = len(e.get("actions", []))
        self.scribe.write(
            self.machine,
            f"SRE: {n} action(s) proposed for {e.get('trigger','')}",
            auto=True, urgency=7
        )

    def _on_flash_proc(self, e):
        self.nucleo.submit("flash_proc", proc=e.get("proc", ""))

    # ── Background loops ──────────────────────────────────────────────────────
    def _forecast_loop(self):
        while True:
            try:
                self.pulse.beat("atlas")
                fc_cpu = self.atlas.forecast("cpu", 300)
                fc_ram = self.atlas.forecast("ram", 300)
                self._forecasts = {"cpu": fc_cpu, "ram": fc_ram}
                for metric, fc in [("cpu", fc_cpu), ("ram", fc_ram)]:
                    if fc.get("minutes_to_90"):
                        self.nucleo.submit(
                            "atlas_breach", metric=metric,
                            context={"minutes_to_90": fc["minutes_to_90"]}
                        )
            except Exception:
                pass
            time.sleep(30)

    def _pulse_loop(self):
        while True:
            self.pulse.beat("helix")
            self.pulse.beat("nucleo")
            self.pulse.beat("grid")
            time.sleep(5)

    # ── Main tick ─────────────────────────────────────────────────────────────
    def process_tick(self, state: dict):
        """Call every 1s collection tick. Returns max urgency seen this tick."""
        cpu = state.get("cpu_pct", 0.0)
        ram = state.get("ram_pct", 0.0)
        ctx = {"cpu_pct": cpu, "ram_pct": ram}

        # Feed all subsystems
        self.prism_cpu.push(cpu)
        self.prism_ram.push(ram)
        self.deox.update_veil("cpu", cpu)
        self.deox.update_veil("ram", ram)
        self.tte.record("cpu", cpu)
        self.tte.record("ram", ram)
        self.atlas.push("cpu", cpu)
        self.atlas.push("ram", ram)
        self.shard.record(self.machine, "cpu", cpu)
        self.shard.record(self.machine, "ram", ram)

        # Route metric signals
        self._last_urgency = 0
        u_cpu = self.nucleo.submit("metric_update", val=cpu, metric="cpu", context=ctx)
        u_ram = self.nucleo.submit("metric_update", val=ram, metric="ram", context=ctx)
        self._last_urgency = max(u_cpu, u_ram)

        # Process genealogy + causal log
        for proc, val in state.get("cpu_top", []):
            self.trace.record_proc(proc, val, 0.0)
            self.trace.record_event("cpu", proc, val)
            self.echo.update_top_procs(state.get("cpu_top", []))

        for proc, val in state.get("ram_top", []):
            self.trace.record_proc(proc, 0.0, val)
            self.trace.record_event("ram", proc, val)

        # Wire DEOX self-heal: detect resolved anomalies
        current_anomalies = {
            (a["proc"], a["metric"])
            for a in state.get("active_anomalies", [])
        }
        for key in self._anomaly_active - current_anomalies:
            proc, metric = key
            self.deox.record_self_heal(metric, proc)
            self.trace.mark_incident(proc)
            self.echo.record_incident()
            self.nucleo.submit("anomaly_resolved", metric=metric, proc=proc, context=ctx)
        self._anomaly_active = current_anomalies

        # TBE floor threats
        for threat in self.tbe.threatens_floor(cpu, ram):
            u = self.nucleo.submit("tbe_floor_threat", val=max(cpu, ram), context=ctx)
            self._last_urgency = max(self._last_urgency, u)
            self.scribe.write(self.machine, f"FLOOR: {threat}", auto=True, urgency=9)

        # FLUX diffs
        for diff in self.flux.diff(self.machine, state):
            self.nucleo.submit(
                "flux_diff",
                context={**ctx, "diff_type": diff["type"]}
            )

        # PM2 crashes → SRE proposals
        for p in state.get("pm2_processes", []):
            if p.get("new_crashes", 0) > 0:
                u = self.nucleo.submit("pm2_crash", proc=p["name"], context=ctx)
                self._last_urgency = max(self._last_urgency, u)
                actions = self.sre.propose("service_down", {**ctx, "proc": p["name"]})
                if actions:
                    self.grid.publish("sre_proposal", {
                        "machine": self.machine,
                        "actions": actions,
                        "trigger": "service_down"
                    })

        self.pulse.beat("collection")
        return self._last_urgency

    def pop_ai_forced(self) -> bool:
        v, self._ai_forced = self._ai_forced, False
        return v

    def get_export(self) -> dict:
        return {
            "forecasts":     self._forecasts,
            "priority_feed": list(self._priority_feed)[:20],
            "queue_depth":   self.nucleo.get_queue_depth(),
            "pulse":         self.pulse.check(),
            "vie":           self._vie_state,
            "annotations":   self.scribe.get_all(self.machine, 30),
            "sre_pending":   self.sre.get_pending(),
            "sre_history":   self.sre.get_history()[-10:],
            "flash_procs":   self.micro.get_recent_flashes(5),
            "new_procs":     self.trace.get_new_procs(),
            "theta":         self.theta.context(),
        }

    def flush_session(self, peak_cpu: float, peak_ram: float,
                      service_restarts: int = 0):
        tag = self.echo.auto_tag_session(peak_cpu, peak_ram, service_restarts)
        self.echo.flush(peak_cpu, peak_ram, tag)

