#!/usr/bin/env python3
"""
SYSWATCH V3 — Intelligence Engine
Handles: anomaly fingerprinting, process reputation, causality detection,
         quiet period learning, cross-session SQLite memory, scoring
"""

import sqlite3, time, statistics, os
from datetime import datetime
from collections import defaultdict, deque

DB_PATH = os.path.expanduser("~/.syswatch_v3.db")

# ── DATABASE SETUP ────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS process_baselines (
        machine     TEXT NOT NULL,
        proc_name   TEXT NOT NULL,
        metric      TEXT NOT NULL,   -- 'cpu' or 'ram'
        samples     INTEGER DEFAULT 0,
        mean        REAL DEFAULT 0,
        m2          REAL DEFAULT 0,  -- Welford M2 for online stddev
        last_seen   REAL DEFAULT 0,
        PRIMARY KEY (machine, proc_name, metric)
    );

    CREATE TABLE IF NOT EXISTS anomaly_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        machine     TEXT NOT NULL,
        proc_name   TEXT NOT NULL,
        metric      TEXT NOT NULL,
        peak_val    REAL NOT NULL,
        baseline    REAL NOT NULL,
        multiplier  REAL NOT NULL,
        started_at  REAL NOT NULL,
        ended_at    REAL,
        duration_s  REAL,
        resolved    INTEGER DEFAULT 0,
        label       TEXT
    );

    CREATE TABLE IF NOT EXISTS session_scores (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        machine     TEXT NOT NULL,
        started_at  REAL NOT NULL,
        ended_at    REAL,
        score       REAL,
        spike_count INTEGER DEFAULT 0,
        peak_cpu    REAL DEFAULT 0,
        peak_ram    REAL DEFAULT 0,
        notes       TEXT
    );

    CREATE TABLE IF NOT EXISTS quiet_periods (
        machine     TEXT NOT NULL,
        hour        INTEGER NOT NULL,   -- 0-23
        idle_count  INTEGER DEFAULT 0,
        active_count INTEGER DEFAULT 0,
        PRIMARY KEY (machine, hour)
    );
    """)
    con.commit()
    con.close()

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

# ── WELFORD ONLINE MEAN/STDDEV ────────────────────────────────────────────────
def welford_update(mean, m2, n, val):
    """Returns (new_mean, new_m2, new_n) using Welford's algorithm."""
    n    += 1
    delta = val - mean
    mean += delta / n
    m2   += delta * (val - mean)
    return mean, m2, n

def welford_stddev(m2, n):
    return (m2 / max(1, n - 1)) ** 0.5 if n >= 2 else 0.0

# ── PROCESS REPUTATION ────────────────────────────────────────────────────────
class ProcessReputation:
    """Tracks per-process baselines, persisted to SQLite."""

    def __init__(self, machine: str):
        self.machine = machine
        # In-memory cache: {(proc, metric): {mean, m2, n}}
        self._cache = {}
        self._load_from_db()

    def _load_from_db(self):
        con = get_db()
        rows = con.execute(
            "SELECT proc_name, metric, samples, mean, m2 FROM process_baselines WHERE machine=?",
            (self.machine,)
        ).fetchall()
        con.close()
        for r in rows:
            self._cache[(r["proc_name"], r["metric"])] = {
                "mean": r["mean"], "m2": r["m2"], "n": r["samples"]
            }

    def update(self, proc: str, metric: str, val: float):
        key = (proc, metric)
        if key not in self._cache:
            self._cache[key] = {"mean": 0.0, "m2": 0.0, "n": 0}
        c = self._cache[key]
        c["mean"], c["m2"], c["n"] = welford_update(c["mean"], c["m2"], c["n"], val)

    def flush_to_db(self):
        con = get_db()
        now = time.time()
        for (proc, metric), c in self._cache.items():
            con.execute("""
                INSERT INTO process_baselines (machine, proc_name, metric, samples, mean, m2, last_seen)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(machine, proc_name, metric) DO UPDATE SET
                    samples=excluded.samples, mean=excluded.mean,
                    m2=excluded.m2, last_seen=excluded.last_seen
            """, (self.machine, proc, metric, c["n"], c["mean"], c["m2"], now))
        con.commit()
        con.close()

    def get_baseline(self, proc: str, metric: str):
        """Returns (mean, stddev, n) or None if insufficient data."""
        c = self._cache.get((proc, metric))
        if not c or c["n"] < 10:
            return None
        return c["mean"], welford_stddev(c["m2"], c["n"]), c["n"]

    def anomaly_multiplier(self, proc: str, metric: str, val: float):
        """Returns how many x above baseline this value is, or None."""
        b = self.get_baseline(proc, metric)
        if not b or b[0] < 0.5:
            return None
        mean, stddev, _ = b
        if val > mean + max(2 * stddev, mean * 0.5):
            return round(val / mean, 2)
        return None

    def describe(self, proc: str, metric: str, val: float) -> str:
        """Human-readable reputation description."""
        b = self.get_baseline(proc, metric)
        if not b:
            return ""
        mean, stddev, n = b
        mult = val / max(0.01, mean)
        unit = "%" if metric == "cpu" else "MB"
        if mult >= 2.0:
            return f"{proc} normally uses {mean:.0f}{unit} — currently {mult:.1f}x above baseline"
        if mult <= 0.3:
            return f"{proc} is unusually quiet (normally {mean:.0f}{unit})"
        return ""

# ── ANOMALY FINGERPRINTING ────────────────────────────────────────────────────
class AnomalyTracker:
    """Detects, names, and tracks anomaly events."""

    def __init__(self, machine: str, reputation: ProcessReputation):
        self.machine    = machine
        self.reputation = reputation
        # Active spikes: {(proc, metric): {db_id, started_at, peak}}
        self._active = {}
        self._recent_events = deque(maxlen=50)
        self._load_recent()

    def _load_recent(self):
        con = get_db()
        rows = con.execute("""
            SELECT * FROM anomaly_events
            WHERE machine=? AND started_at > ?
            ORDER BY started_at DESC LIMIT 50
        """, (self.machine, time.time() - 86400)).fetchall()
        con.close()
        for r in rows:
            self._recent_events.append(dict(r))

    def check(self, proc: str, metric: str, val: float):
        key = (proc, metric)
        mult = self.reputation.anomaly_multiplier(proc, metric, val)

        if mult and mult >= 1.8:
            # Spike started or continuing
            if key not in self._active:
                # New spike — insert to DB
                con = get_db()
                b = self.reputation.get_baseline(proc, metric)
                baseline = b[0] if b else 0
                cur = con.execute("""
                    INSERT INTO anomaly_events
                    (machine, proc_name, metric, peak_val, baseline, multiplier, started_at)
                    VALUES (?,?,?,?,?,?,?)
                """, (self.machine, proc, metric, val, baseline, mult, time.time()))
                db_id = cur.lastrowid
                con.commit(); con.close()
                self._active[key] = {"db_id": db_id, "started_at": time.time(), "peak": val}
            else:
                # Update peak
                if val > self._active[key]["peak"]:
                    self._active[key]["peak"] = val
                    con = get_db()
                    con.execute("UPDATE anomaly_events SET peak_val=?, multiplier=? WHERE id=?",
                                (val, mult, self._active[key]["db_id"]))
                    con.commit(); con.close()
        else:
            # Spike resolved
            if key in self._active:
                a     = self._active.pop(key)
                dur   = round(time.time() - a["started_at"], 1)
                con   = get_db()
                con.execute("""
                    UPDATE anomaly_events
                    SET ended_at=?, duration_s=?, resolved=1
                    WHERE id=?
                """, (time.time(), dur, a["db_id"]))
                con.commit(); con.close()
                self._load_recent()

    def get_pattern_summary(self, proc: str, metric: str) -> str:
        """Summarize historical pattern for this proc+metric."""
        events = [e for e in self._recent_events
                  if e["proc_name"] == proc and e["metric"] == metric and e["resolved"]]
        if len(events) < 2:
            return ""
        count = len(events)
        durations = [e["duration_s"] for e in events if e["duration_s"]]
        avg_dur   = statistics.mean(durations) if durations else 0
        unit      = "%" if metric == "cpu" else "MB"
        peaks     = [e["peak_val"] for e in events]
        avg_peak  = statistics.mean(peaks)

        summary = f"{proc} has spiked {count}x today"
        if avg_dur:
            summary += f", avg duration {avg_dur:.0f}s"
        if avg_dur and avg_dur < 300:
            summary += " (self-resolving)"
        summary += f", avg peak {avg_peak:.0f}{unit}"
        return summary

    def get_active_anomalies(self) -> list:
        out = []
        for (proc, metric), a in self._active.items():
            dur = round(time.time() - a["started_at"])
            out.append({
                "proc": proc, "metric": metric,
                "peak": a["peak"], "duration_s": dur,
                "db_id": a["db_id"]
            })
        return out

    def get_recent_events(self, limit=10) -> list:
        return list(self._recent_events)[-limit:]

# ── CAUSALITY DETECTION ───────────────────────────────────────────────────────
class CausalityEngine:
    """Detects correlations between process presence and metric spikes."""

    def __init__(self):
        # {proc_name: deque of (cpu_when_active, ram_when_active)}
        self._proc_impact = defaultdict(lambda: deque(maxlen=120))
        self._baseline_cpu = deque(maxlen=120)
        self._baseline_ram = deque(maxlen=120)

    def record(self, cpu_pct: float, ram_pct: float, active_procs: list):
        """Call every tick with current metrics and top process names."""
        self._baseline_cpu.append(cpu_pct)
        self._baseline_ram.append(ram_pct)
        for proc in active_procs:
            self._proc_impact[proc].append((cpu_pct, ram_pct))

    def get_correlations(self) -> list:
        """Return list of (proc, metric, correlation_str) for strong correlations."""
        if len(self._baseline_cpu) < 30:
            return []
        global_cpu_mean = statistics.mean(self._baseline_cpu)
        global_ram_mean = statistics.mean(self._baseline_ram)
        results = []
        for proc, samples in self._proc_impact.items():
            if len(samples) < 15:
                continue
            cpu_vals = [s[0] for s in samples]
            ram_vals = [s[1] for s in samples]
            cpu_when_active = statistics.mean(cpu_vals)
            ram_when_active = statistics.mean(ram_vals)
            cpu_lift = cpu_when_active - global_cpu_mean
            ram_lift = ram_when_active - global_ram_mean
            if cpu_lift >= 15:
                results.append({
                    "proc": proc, "metric": "cpu",
                    "lift": round(cpu_lift, 1),
                    "desc": f"CPU runs +{cpu_lift:.0f}% higher when {proc} is active"
                })
            if ram_lift >= 200:
                results.append({
                    "proc": proc, "metric": "ram",
                    "lift": round(ram_lift, 1),
                    "desc": f"RAM runs +{ram_lift:.0f}MB higher when {proc} is active"
                })
        results.sort(key=lambda x: x["lift"], reverse=True)
        return results[:3]

# ── QUIET PERIOD LEARNING ─────────────────────────────────────────────────────
class QuietPeriodTracker:
    """Learns which hours are typically idle vs active."""

    IDLE_THRESHOLD = 15.0  # CPU % below this = idle

    def __init__(self, machine: str):
        self.machine = machine
        self._tick_count = 0

    def record(self, cpu_pct: float):
        self._tick_count += 1
        if self._tick_count % 60 != 0:  # sample once per minute
            return
        hour = datetime.now().hour
        idle = cpu_pct < self.IDLE_THRESHOLD
        con  = get_db()
        if idle:
            con.execute("""
                INSERT INTO quiet_periods (machine, hour, idle_count, active_count)
                VALUES (?,?,1,0)
                ON CONFLICT(machine, hour) DO UPDATE SET idle_count=idle_count+1
            """, (self.machine, hour))
        else:
            con.execute("""
                INSERT INTO quiet_periods (machine, hour, idle_count, active_count)
                VALUES (?,?,0,1)
                ON CONFLICT(machine, hour) DO UPDATE SET active_count=active_count+1
            """, (self.machine, hour))
        con.commit(); con.close()

    def is_quiet_hour(self) -> bool:
        hour = datetime.now().hour
        con  = get_db()
        row  = con.execute(
            "SELECT idle_count, active_count FROM quiet_periods WHERE machine=? AND hour=?",
            (self.machine, hour)
        ).fetchone()
        con.close()
        if not row or (row["idle_count"] + row["active_count"]) < 5:
            return False
        return row["idle_count"] > row["active_count"]

    def get_quiet_hours(self) -> list:
        con  = get_db()
        rows = con.execute(
            "SELECT hour, idle_count, active_count FROM quiet_periods WHERE machine=?",
            (self.machine,)
        ).fetchall()
        con.close()
        return [r["hour"] for r in rows
                if r["idle_count"] > r["active_count"] and (r["idle_count"]+r["active_count"]) >= 5]

# ── SCORING ENGINE ────────────────────────────────────────────────────────────
class ScoringEngine:
    """Computes a 0–1000 session health score."""

    def __init__(self, machine: str):
        self.machine      = machine
        self.session_start = time.time()
        self._session_id  = None
        self._spike_count = 0
        self._init_session()

    def _init_session(self):
        con = get_db()
        cur = con.execute(
            "INSERT INTO session_scores (machine, started_at) VALUES (?,?)",
            (self.machine, self.session_start)
        )
        self._session_id = cur.lastrowid
        con.commit(); con.close()

    def record_spike(self):
        self._spike_count += 1

    def compute(self, peak_cpu: float, peak_ram: float,
                thermal_risk: bool, anomaly_count: int) -> dict:
        score = 1000.0

        # Spike penalty (up to -300)
        score -= min(300, self._spike_count * 25)

        # Peak CPU penalty (up to -200)
        if peak_cpu > 90:   score -= 200
        elif peak_cpu > 75: score -= 100
        elif peak_cpu > 60: score -= 50
        elif peak_cpu > 40: score -= 20

        # Peak RAM penalty (up to -200)
        if peak_ram > 90:   score -= 200
        elif peak_ram > 80: score -= 100
        elif peak_ram > 70: score -= 50
        elif peak_ram > 60: score -= 20

        # Thermal penalty
        if thermal_risk: score -= 100

        # Anomaly penalty (up to -200)
        score -= min(200, anomaly_count * 40)

        score = max(0, round(score))

        # Grade
        if score >= 900: grade = "S"
        elif score >= 800: grade = "A"
        elif score >= 700: grade = "B"
        elif score >= 600: grade = "C"
        elif score >= 500: grade = "D"
        else: grade = "F"

        return {"score": score, "grade": grade, "spikes": self._spike_count}

    def flush(self, score: int, peak_cpu: float, peak_ram: float):
        con = get_db()
        con.execute("""
            UPDATE session_scores
            SET ended_at=?, score=?, spike_count=?, peak_cpu=?, peak_ram=?
            WHERE id=?
        """, (time.time(), score, self._spike_count, peak_cpu, peak_ram, self._session_id))
        con.commit(); con.close()

    def get_history(self, limit=5) -> list:
        con  = get_db()
        rows = con.execute("""
            SELECT score, spike_count, peak_cpu, peak_ram, started_at
            FROM session_scores
            WHERE machine=? AND score IS NOT NULL
            ORDER BY started_at DESC LIMIT ?
        """, (self.machine, limit)).fetchall()
        con.close()
        return [dict(r) for r in rows]

# ── INTELLIGENCE CONTEXT FOR AI PROMPTS ──────────────────────────────────────
def build_intelligence_context(machine: str, reputation: ProcessReputation,
                                anomaly: AnomalyTracker, causality: CausalityEngine,
                                quiet: QuietPeriodTracker, score: ScoringEngine,
                                cpu_top: list, ram_top: list) -> str:
    lines = []

    # Active anomalies
    active = anomaly.get_active_anomalies()
    if active:
        lines.append("ACTIVE ANOMALIES:")
        for a in active:
            pattern = anomaly.get_pattern_summary(a["proc"], a["metric"])
            lines.append(f"  {a['proc']} {a['metric'].upper()} spike: {a['peak']:.0f} for {a['duration_s']}s. {pattern}")

    # Process reputation flags
    rep_flags = []
    for proc, cpu in cpu_top[:4]:
        desc = reputation.describe(proc, "cpu", cpu)
        if desc: rep_flags.append(desc)
    for proc, ram in ram_top[:4]:
        desc = reputation.describe(proc, "ram", ram)
        if desc: rep_flags.append(desc)
    if rep_flags:
        lines.append("REPUTATION FLAGS:")
        for f in rep_flags: lines.append(f"  {f}")

    # Causality
    corrs = causality.get_correlations()
    if corrs:
        lines.append("CAUSALITY:")
        for c in corrs: lines.append(f"  {c['desc']}")

    # Quiet period
    if quiet.is_quiet_hour():
        lines.append("NOTE: This is typically a quiet hour for this machine.")

    # Score
    s = score.compute(0, 0, False, len(active))
    lines.append(f"SESSION SCORE: {s['score']}/1000 (Grade {s['grade']})")

    return "\n".join(lines) if lines else ""
