#!/usr/bin/env python3
"""
SYSWATCH V5 — Extended Systems Module
SPECTER · MERIDIAN · CORONA · VIGIL · PRAXIS · LUMEN · SENTINEL
VECTOR · CODEX · NEXUS · MOSAIC · PDCM · PHANTOM · ORACLE_PRED · LENS
All systems are invisible to the UI — they feed data upward only.
"""

import threading, time, json, sqlite3, os, math, re, subprocess, sys
from collections import deque, defaultdict
from datetime import datetime
from urllib.request import urlopen, Request

DB = os.path.expanduser("~/.syswatch_systems.db")

def _db():
    return sqlite3.connect(DB, check_same_thread=False)

def _init_db():
    c = _db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS codex (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL, category TEXT, machine TEXT, summary TEXT, data TEXT
    );
    CREATE TABLE IF NOT EXISTS pdcm_prefs (
        key TEXT PRIMARY KEY, value TEXT, updated REAL
    );
    CREATE TABLE IF NOT EXISTS vigil_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL, threat TEXT, score INTEGER, resolved INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS phantom_procs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL, machine TEXT, name TEXT, pid INTEGER, duration_ms REAL
    );
    CREATE TABLE IF NOT EXISTS nexus_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL, src TEXT, dst TEXT, edge_type TEXT, weight REAL
    );
    """)
    c.commit()
    c.close()

_init_db()

# ── VECTOR — directional intelligence ────────────────────────────────────────
class VECTOR:
    """Adds velocity and acceleration to every metric."""
    def __init__(self, window=10):
        self._bufs = defaultdict(lambda: deque(maxlen=window))
        self._lock = threading.Lock()

    def update(self, key, value):
        with self._lock:
            self._bufs[key].append((time.time(), value))

    def get_direction(self, key):
        with self._lock:
            buf = list(self._bufs[key])
        if len(buf) < 3:
            return {"velocity": 0.0, "acceleration": 0.0, "trend": "stable"}
        vals = [v for _, v in buf]
        # Simple linear regression for velocity
        n = len(vals)
        x_mean = (n - 1) / 2
        y_mean = sum(vals) / n
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(vals))
        den = sum((i - x_mean) ** 2 for i in range(n))
        velocity = num / den if den else 0.0
        # Acceleration = change in velocity
        mid = n // 2
        v1 = (vals[mid] - vals[0]) / mid if mid else 0
        v2 = (vals[-1] - vals[mid]) / (n - mid) if (n - mid) else 0
        acceleration = v2 - v1
        trend = "rising" if velocity > 0.5 else "falling" if velocity < -0.5 else "stable"
        return {
            "velocity": round(velocity, 3),
            "acceleration": round(acceleration, 3),
            "trend": trend,
            "current": vals[-1],
            "min": min(vals),
            "max": max(vals),
        }

# ── PDCM — personal data collection module ───────────────────────────────────
class PDCM:
    """Learns user preferences and behavior patterns silently."""
    def __init__(self):
        self._lock  = threading.Lock()
        self._prefs = self._load()
        # Behavior tracking
        self._dismissed_alerts = defaultdict(int)
        self._acted_alerts     = defaultdict(int)
        self._active_hours     = defaultdict(int)  # hour -> activity count
        self._proc_interest    = defaultdict(float) # proc -> interest score

    def _load(self):
        try:
            c = _db()
            rows = c.execute("SELECT key, value FROM pdcm_prefs").fetchall()
            c.close()
            return {k: json.loads(v) for k, v in rows}
        except:
            return {}

    def _save(self, key, value):
        try:
            c = _db()
            c.execute("INSERT OR REPLACE INTO pdcm_prefs (key, value, updated) VALUES (?,?,?)",
                      (key, json.dumps(value), time.time()))
            c.commit()
            c.close()
        except:
            pass

    def record_alert_dismissed(self, alert_type):
        with self._lock:
            self._dismissed_alerts[alert_type] += 1

    def record_alert_acted(self, alert_type):
        with self._lock:
            self._acted_alerts[alert_type] += 1

    def record_activity(self):
        hour = datetime.now().hour
        with self._lock:
            self._active_hours[hour] += 1

    def record_proc_view(self, proc_name):
        with self._lock:
            self._proc_interest[proc_name] = self._proc_interest.get(proc_name, 0) + 1.0

    def get_alert_weight(self, alert_type):
        """Returns 0.0-2.0 multiplier for how much user cares about this alert type."""
        with self._lock:
            dismissed = self._dismissed_alerts.get(alert_type, 0)
            acted     = self._acted_alerts.get(alert_type, 0)
            total     = dismissed + acted
            if total == 0:
                return 1.0
            act_rate = acted / total
            return round(0.2 + 1.8 * act_rate, 2)

    def is_active_hour(self, hour=None):
        hour = hour or datetime.now().hour
        with self._lock:
            avg = sum(self._active_hours.values()) / max(len(self._active_hours), 1)
            return self._active_hours.get(hour, 0) >= avg * 0.5

    def get_top_procs(self, n=5):
        with self._lock:
            return sorted(self._proc_interest.items(), key=lambda x: -x[1])[:n]

    def get_profile(self):
        now_hour = datetime.now().hour
        with self._lock:
            return {
                "active_now": self.is_active_hour(now_hour),
                "peak_hours": sorted(self._active_hours, key=lambda h: -self._active_hours[h])[:3],
                "top_procs": self.get_top_procs(),
                "alert_weights": dict(self._dismissed_alerts),
            }

# ── SPECTER — passive device identity engine ─────────────────────────────────
class SPECTER:
    """Builds behavioral fingerprints for devices without explicit labeling."""
    def __init__(self):
        self._lock       = threading.Lock()
        self._behaviors  = defaultdict(lambda: {
            "active_hours": defaultdict(int),
            "idle_count": 0,
            "active_count": 0,
            "seen_count": 0,
            "last_seen": 0,
            "inferred_type": "unknown",
        })

    def observe(self, mac, is_present, hour=None):
        hour = hour or datetime.now().hour
        with self._lock:
            b = self._behaviors[mac]
            b["seen_count"] += 1
            b["last_seen"]   = time.time()
            if is_present:
                b["active_count"] += 1
                b["active_hours"][hour] += 1
            else:
                b["idle_count"] += 1
            # Infer type from behavior
            total = b["active_count"] + b["idle_count"]
            if total > 20:
                active_ratio = b["active_count"] / total
                peak_hours   = sorted(b["active_hours"], key=lambda h: -b["active_hours"][h])[:3]
                night_active = any(h in range(22, 24) or h in range(0, 6) for h in peak_hours)
                if active_ratio > 0.8:
                    b["inferred_type"] = "always_on_device"  # IoT, TV, router
                elif night_active and active_ratio < 0.6:
                    b["inferred_type"] = "person_phone"
                elif active_ratio > 0.4:
                    b["inferred_type"] = "laptop_or_phone"
                else:
                    b["inferred_type"] = "intermittent_device"

    def get_inference(self, mac):
        with self._lock:
            return dict(self._behaviors.get(mac, {}))

    def get_all_inferences(self):
        with self._lock:
            return {mac: dict(b) for mac, b in self._behaviors.items()}

# ── MERIDIAN — spatial/floor intelligence ────────────────────────────────────
class MERIDIAN:
    """Translates BT RSSI signals into floor estimates using Kalman filtering."""
    # RSSI thresholds for floor detection (Mac on floor 4)
    FLOOR_THRESHOLDS = [
        (4, -65),   # floor 4: strong signal
        (3, -75),   # floor 3: medium
        (2, -82),   # floor 2: weak
        (1, -200),  # floor 1: very weak or none
    ]

    def __init__(self):
        self._lock        = threading.Lock()
        self._rssi_bufs   = defaultdict(lambda: deque(maxlen=8))
        self._floor_state = {}  # mac -> {floor, confidence, ts}
        self._overrides   = {}  # mac -> floor (manual)
        self._history     = defaultdict(list)

    def record_rssi(self, mac, rssi):
        with self._lock:
            self._rssi_bufs[mac].append(rssi)
            # Kalman-ish: weighted average (recent samples weighted more)
            buf    = list(self._rssi_bufs[mac])
            n      = len(buf)
            weights = [math.exp(0.3 * i) for i in range(n)]
            w_sum   = sum(weights)
            smooth  = sum(w * v for w, v in zip(weights, buf)) / w_sum
            # Map to floor
            floor = 1
            for fl, threshold in self.FLOOR_THRESHOLDS:
                if smooth >= threshold:
                    floor = fl
                    break
            # Confidence based on signal consistency
            if n >= 3:
                variance   = sum((v - smooth) ** 2 for v in buf) / n
                confidence = max(0, min(100, int(100 - variance * 2)))
            else:
                confidence = 30
            prev = self._floor_state.get(mac, {})
            if prev.get("floor") != floor:
                self._history[mac].append({
                    "from": prev.get("floor"), "to": floor,
                    "ts": time.time(), "confidence": confidence
                })
            self._floor_state[mac] = {
                "floor": floor, "rssi": round(smooth, 1),
                "confidence": confidence, "ts": time.time(),
                "method": "bluetooth"
            }

    def set_override(self, mac, floor):
        with self._lock:
            self._overrides[mac] = floor
            self._floor_state[mac] = {
                "floor": floor, "rssi": None,
                "confidence": 100, "ts": time.time(),
                "method": "manual"
            }

    def get_floor(self, mac):
        with self._lock:
            if mac in self._overrides:
                return self._floor_state.get(mac, {})
            return self._floor_state.get(mac, {"floor": None, "confidence": 0})

    def get_all_floors(self):
        with self._lock:
            return dict(self._floor_state)

    def get_history(self, mac):
        with self._lock:
            return list(self._history.get(mac, []))

# ── VIGIL — threat and anomaly watchdog ──────────────────────────────────────
class VIGIL:
    """Watches for genuinely anomalous events across all data sources."""
    def __init__(self):
        self._lock    = threading.Lock()
        self._threats = deque(maxlen=100)
        self._seen    = set()  # deduplicate

    def check_network(self, devices, known_macs):
        threats = []
        now = time.time()
        for d in devices:
            mac = d.get("mac", "")
            if mac not in known_macs and d.get("device_type") == "unknown":
                key = f"unknown_{mac}"
                if key not in self._seen:
                    self._seen.add(key)
                    threats.append({
                        "type": "UNKNOWN_DEVICE",
                        "msg":  f"Unidentified device on network: {d.get('ip')} ({mac})",
                        "score": 7, "ts": now, "resolved": False
                    })
        hour = datetime.now().hour
        if 0 <= hour < 5:
            for d in devices:
                if d.get("is_phone") and d.get("mac") not in known_macs:
                    key = f"night_unknown_{d['mac']}"
                    if key not in self._seen:
                        self._seen.add(key)
                        threats.append({
                            "type": "NIGHT_UNKNOWN_PHONE",
                            "msg":  f"Unknown phone on network at {hour:02d}:00",
                            "score": 9, "ts": now, "resolved": False
                        })
        with self._lock:
            for t in threats:
                self._threats.appendleft(t)
                try:
                    c = _db()
                    c.execute("INSERT INTO vigil_log (ts, threat, score) VALUES (?,?,?)",
                              (t["ts"], t["msg"], t["score"]))
                    c.commit()
                    c.close()
                except:
                    pass
        return threats

    def check_system(self, state, machine):
        threats = []
        now     = time.time()
        cpu     = state.get("cpu_pct", 0)
        ram     = state.get("ram_pct", 0)
        # Sustained high CPU
        if cpu > 95:
            key = f"cpu95_{machine}"
            if key not in self._seen:
                self._seen.add(key)
                threats.append({
                    "type": "CRITICAL_CPU",
                    "msg":  f"{machine} CPU at {cpu:.0f}% — critically high",
                    "score": 9, "ts": now, "resolved": False
                })
        # RAM near OOM
        if ram > 92:
            key = f"ram92_{machine}"
            if key not in self._seen:
                self._seen.add(key)
                threats.append({
                    "type": "CRITICAL_RAM",
                    "msg":  f"{machine} RAM at {ram:.0f}% — near OOM",
                    "score": 9, "ts": now, "resolved": False
                })
        with self._lock:
            for t in threats:
                self._threats.appendleft(t)
        return threats

    def resolve(self, threat_type):
        with self._lock:
            for t in self._threats:
                if t["type"] == threat_type:
                    t["resolved"] = True
            self._seen.discard(threat_type)

    def get_active(self):
        with self._lock:
            return [t for t in self._threats if not t["resolved"]]

    def get_all(self, limit=20):
        with self._lock:
            return list(self._threats)[:limit]

# ── SENTINEL — perimeter watchdog ────────────────────────────────────────────
class SENTINEL:
    """Monitors network edge — new outbound IPs, unusual traffic patterns."""
    def __init__(self):
        self._lock         = threading.Lock()
        self._known_ips    = set()
        self._new_ips      = deque(maxlen=50)
        self._last_pub_ip  = None
        self._pub_ip_ts    = 0

    def check_connections(self, connections):
        """connections: list of {proc, addr, status}"""
        alerts = []
        now = time.time()
        for c in connections:
            addr = c.get("addr", "")
            if not addr or addr.startswith("127.") or addr.startswith("192.168."):
                continue
            ip = addr.split(":")[0] if ":" in addr else addr
            if ip and ip not in self._known_ips:
                self._known_ips.add(ip)
                entry = {"ip": ip, "proc": c.get("proc", "unknown"), "ts": now}
                with self._lock:
                    self._new_ips.appendleft(entry)
                alerts.append(entry)
        return alerts

    def get_public_ip(self):
        now = time.time()
        if now - self._pub_ip_ts < 300:
            return self._last_pub_ip
        try:
            with urlopen("https://api.ipify.org?format=json", timeout=5) as r:
                data = json.loads(r.read())
                ip   = data.get("ip")
                if ip != self._last_pub_ip and self._last_pub_ip:
                    pass  # IP changed — could alert
                self._last_pub_ip = ip
                self._pub_ip_ts   = now
                return ip
        except:
            return self._last_pub_ip

    def get_new_connections(self, limit=10):
        with self._lock:
            return list(self._new_ips)[:limit]

# ── CORONA — world event correlator ──────────────────────────────────────────
class CORONA:
    """Connects world events to system behavior."""
    def __init__(self):
        self._lock    = threading.Lock()
        self._events  = deque(maxlen=200)
        self._corr    = []  # found correlations

    def ingest_news(self, news_items):
        now = time.time()
        with self._lock:
            for item in news_items[:5]:
                self._events.append({
                    "type": "news", "text": item.get("headline", ""),
                    "source": item.get("source", ""), "ts": now
                })

    def ingest_system_event(self, machine, event_type, value):
        with self._lock:
            self._events.append({
                "type": "system", "machine": machine,
                "event": event_type, "value": value,
                "ts": time.time()
            })

    def get_recent(self, limit=20):
        with self._lock:
            return list(self._events)[:limit]

# ── CODEX — institutional memory ─────────────────────────────────────────────
class CODEX:
    """Permanent indexed memory of everything that happened."""
    def __init__(self):
        self._lock = threading.Lock()

    def record(self, category, machine, summary, data=None):
        try:
            c = _db()
            c.execute(
                "INSERT INTO codex (ts, category, machine, summary, data) VALUES (?,?,?,?,?)",
                (time.time(), category, machine, summary, json.dumps(data or {}))
            )
            c.commit()
            c.close()
        except:
            pass

    def query(self, category=None, machine=None, limit=50, since=None):
        try:
            c    = _db()
            sql  = "SELECT ts, category, machine, summary, data FROM codex WHERE 1=1"
            args = []
            if category:
                sql += " AND category=?"; args.append(category)
            if machine:
                sql += " AND machine=?"; args.append(machine)
            if since:
                sql += " AND ts>?"; args.append(since)
            sql += f" ORDER BY ts DESC LIMIT {limit}"
            rows = c.execute(sql, args).fetchall()
            c.close()
            return [{"ts": r[0], "category": r[1], "machine": r[2],
                     "summary": r[3], "data": json.loads(r[4])} for r in rows]
        except:
            return []

    def ask(self, question):
        """Answer questions about history from CODEX records."""
        records = self.query(limit=100)
        if not records:
            return "No historical data yet."
        # Simple keyword matching
        q = question.lower()
        relevant = [r for r in records
                    if any(w in r["summary"].lower() for w in q.split() if len(w) > 3)]
        if not relevant:
            return f"No matching records found for: {question}"
        return f"Found {len(relevant)} relevant records. Most recent: {relevant[0]['summary']}"

# ── LENS — attention/focus engine ────────────────────────────────────────────
class LENS:
    """Scores everything by how much it deserves attention right now."""
    def __init__(self, pdcm: PDCM):
        self._pdcm   = pdcm
        self._lock   = threading.Lock()
        self._scores = {}

    def score(self, item_id, base_score, item_type, metadata=None):
        """Score an item 0-100 for attention worthiness."""
        weight  = self._pdcm.get_alert_weight(item_type)
        active  = self._pdcm.is_active_hour()
        # During off-hours, only truly critical things get attention
        hour_mult = 1.0 if active else 0.5
        final = min(100, base_score * weight * hour_mult)
        with self._lock:
            self._scores[item_id] = {"score": final, "type": item_type, "ts": time.time()}
        return final

    def get_top(self, n=5):
        with self._lock:
            sorted_items = sorted(self._scores.items(), key=lambda x: -x[1]["score"])
            return sorted_items[:n]

    def get_focus_item(self):
        """The single most important thing right now."""
        top = self.get_top(1)
        return top[0] if top else None

# ── MOSAIC — meta-pattern synthesis ──────────────────────────────────────────
class MOSAIC:
    """Finds patterns across all systems that no single system would catch."""
    def __init__(self):
        self._lock    = threading.Lock()
        self._patterns = deque(maxlen=50)
        self._obs      = deque(maxlen=500)  # raw observations

    def observe(self, source, event, value, ts=None):
        with self._lock:
            self._obs.append({
                "source": source, "event": event,
                "value": value, "ts": ts or time.time()
            })

    def find_patterns(self):
        """Look for temporal correlations in observations."""
        with self._lock:
            obs = list(self._obs)
        if len(obs) < 20:
            return []
        patterns = []
        # Look for events that consistently follow each other within 60s
        event_pairs = defaultdict(list)
        for i, o1 in enumerate(obs):
            for o2 in obs[i+1:i+20]:
                if 0 < o2["ts"] - o1["ts"] < 60:
                    key = (o1["event"], o2["event"])
                    event_pairs[key].append(o2["ts"] - o1["ts"])
        for (e1, e2), delays in event_pairs.items():
            if len(delays) >= 3:
                avg_delay = sum(delays) / len(delays)
                patterns.append({
                    "pattern": f"{e1} → {e2}",
                    "occurrences": len(delays),
                    "avg_delay_s": round(avg_delay, 1),
                    "confidence": min(100, len(delays) * 20)
                })
        with self._lock:
            for p in patterns:
                self._patterns.appendleft(p)
        return patterns[:10]

    def get_patterns(self):
        with self._lock:
            return list(self._patterns)[:10]

# ── NEXUS — relationship graph ────────────────────────────────────────────────
class NEXUS:
    """Builds a live graph of relationships between everything."""
    def __init__(self):
        self._lock  = threading.Lock()
        self._nodes = {}   # id -> {label, type, weight}
        self._edges = defaultdict(lambda: defaultdict(float))  # src -> dst -> weight

    def add_node(self, node_id, label, node_type):
        with self._lock:
            self._nodes[node_id] = {"label": label, "type": node_type, "weight": 1.0}

    def add_edge(self, src, dst, weight=1.0, edge_type="related"):
        with self._lock:
            self._edges[src][dst] += weight
            # Persist occasionally
        try:
            c = _db()
            c.execute("INSERT INTO nexus_edges (ts,src,dst,edge_type,weight) VALUES (?,?,?,?,?)",
                      (time.time(), src, dst, edge_type, weight))
            c.commit()
            c.close()
        except:
            pass

    def get_graph(self, max_nodes=30):
        with self._lock:
            nodes = list(self._nodes.items())[:max_nodes]
            edges = []
            for src, dsts in self._edges.items():
                for dst, w in dsts.items():
                    if w > 0.5:
                        edges.append({"src": src, "dst": dst, "weight": round(w, 2)})
            return {"nodes": [{"id": k, **v} for k, v in nodes], "edges": edges[:50]}

# ── ORACLE_PRED — predictive intelligence (renamed from ORACLE to avoid clash)
class ORACLE_PRED:
    """Makes specific time-based predictions from historical patterns."""
    def __init__(self):
        self._lock       = threading.Lock()
        self._predictions = deque(maxlen=20)
        self._accuracy    = deque(maxlen=50)

    def predict(self, metric, history, horizon_minutes=30):
        """Simple linear extrapolation with confidence."""
        if len(history) < 5:
            return None
        vals   = list(history)[-20:]
        n      = len(vals)
        x_mean = (n - 1) / 2
        y_mean = sum(vals) / n
        num    = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(vals))
        den    = sum((i - x_mean) ** 2 for i in range(n))
        slope  = num / den if den else 0
        # Project forward
        steps_per_min = 2  # assuming 30s polling = 2 per minute
        steps          = horizon_minutes * steps_per_min
        projected      = vals[-1] + slope * steps
        projected      = max(0, min(100, projected))
        # Confidence based on R²
        y_pred = [y_mean + slope * (i - x_mean) for i in range(n)]
        ss_res = sum((v - p) ** 2 for v, p in zip(vals, y_pred))
        ss_tot = sum((v - y_mean) ** 2 for v in vals)
        r2     = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        pred   = {
            "metric": metric,
            "current": round(vals[-1], 1),
            "projected": round(projected, 1),
            "horizon_min": horizon_minutes,
            "confidence": round(max(0, r2) * 100),
            "slope_per_min": round(slope * steps_per_min, 3),
            "ts": time.time()
        }
        with self._lock:
            self._predictions.appendleft(pred)
        return pred

    def get_predictions(self):
        with self._lock:
            return list(self._predictions)


# ── BT FLOOR SCANNER ──────────────────────────────────────────────────────────
_bt_rssi_cache = {}   # uuid -> {rssi, name, ts}
_bt_lock       = threading.Lock()
_bt_running    = False
_bt_scan_path  = os.path.join(os.path.dirname(__file__), "bt_scan.py")

# ── BT FLOOR SCANNER (bleak) ─────────────────────────────────────────────────
_bt_rssi_cache = {}   # uuid -> {rssi, name, ts, floor_estimate}
_bt_lock       = threading.Lock()
_bt_running    = False

# Fixed anchor devices with known floor locations
# Used to calibrate RSSI readings dynamically
BT_ANCHORS = {
    "A9E76117-5810-29DC-ED58-7BEF5F7B9713": {"name": "N0231",  "floor": 2},
    "34E81831-CA3D-DE1E-B91B-EBBA6B6506DC": {"name": "N01U7",  "floor": 3},
    "16BB275D-E8A7-ABE4-F3F0-1947D136F75A": {"name": "N02HV",  "floor": 3},
    "316707BA-585F-7E16-D2FF-F6218BD6904C": {"name": "N025W",  "floor": 2},
    "50801846-01A3-7816-884D-09C66732FBC3": {"name": "N01ZJ",  "floor": 3},
    "060B67B6-DE9F-EEA0-7FD0-3DAB768A6AE6": {"name": "Samsung TV", "floor": 1},
    "4C364C0F-EE49-D4ED-B555-7826DE52F122": {"name": "Front Door Lock", "floor": 1},
}

# Calibrated RSSI thresholds (Mac on floor 4)
# Derived from actual scan data across all floors
FLOOR_THRESHOLDS = [
    (4, -65),   # -35 to -65  → Floor 4 (same floor)
    (3, -78),   # -66 to -78  → Floor 3
    (2, -88),   # -79 to -88  → Floor 2
    (1, -200),  # -89+        → Floor 1 or outside
]

def _rssi_to_floor(rssi):
    """Map RSSI value to floor number."""
    for floor, threshold in FLOOR_THRESHOLDS:
        if rssi >= threshold:
            return floor
    return 1

def _calibrate_from_anchors(scan_results):
    """
    Use known anchor devices to validate and adjust floor estimates.
    If anchors are reading higher/lower than expected, shift all estimates.
    """
    offsets = []
    for uuid, anchor in BT_ANCHORS.items():
        if uuid in scan_results:
            rssi = scan_results[uuid]["rssi"]
            # What floor does raw RSSI say?
            raw_floor = _rssi_to_floor(rssi)
            actual_floor = anchor["floor"]
            # Calculate how many floors off we are
            if raw_floor != actual_floor:
                offsets.append(actual_floor - raw_floor)
    # If most anchors agree we're off, apply correction
    if len(offsets) >= 2:
        avg_offset = sum(offsets) / len(offsets)
        return round(avg_offset)
    return 0

def _start_bt_scanner():
    """
    Run BT scanner as a fully isolated subprocess.
    CoreBluetooth on macOS requires the main NSRunLoop thread.
    Running in a subprocess gives it a clean main thread with no conflicts.
    """
    global _bt_running
    if _bt_running:
        return
    if not os.path.exists(_bt_scan_path):
        return
    _bt_running = True

    def _scan_loop():
        while True:
            try:
                result = subprocess.run(
                    [sys.executable, _bt_scan_path],
                    capture_output=True,
                    text=True,
                    timeout=25,
                    # Run in completely isolated environment
                    env={**os.environ, "PYTHONPATH": ""}
                )
                out = result.stdout.strip()
                if out and out.startswith("{"):
                    data = json.loads(out)
                    if isinstance(data, dict) and "error" not in data:
                        now = time.time()
                        fresh = {}
                        for addr, info in data.items():
                            rssi = info.get("rssi", -100)
                            floor_offset = _calibrate_from_anchors(
                                {k: {"rssi": v.get("rssi", -100)} for k, v in data.items()}
                            )
                            fresh[addr] = {
                                "rssi":           rssi,
                                "name":           info.get("name", ""),
                                "ts":             now,
                                "floor_estimate": max(1, min(4, _rssi_to_floor(rssi) + floor_offset)),
                                "floor_offset":   floor_offset,
                            }
                        with _bt_lock:
                            expired = [k for k, v in _bt_rssi_cache.items()
                                       if now - v.get("ts", 0) > 90]
                            for k in expired:
                                del _bt_rssi_cache[k]
                            _bt_rssi_cache.update(fresh)
            except subprocess.TimeoutExpired:
                pass
            except Exception:
                pass
            time.sleep(30)

    t = threading.Thread(target=_scan_loop, daemon=True)
    t.name = "bt-scanner"
    t.start()

def get_bt_rssi():
    with _bt_lock:
        return dict(_bt_rssi_cache)

def get_anchors():
    """Return anchor device status for debugging."""
    with _bt_lock:
        result = {}
        for uuid, anchor in BT_ANCHORS.items():
            if uuid in _bt_rssi_cache:
                result[anchor["name"]] = {
                    "floor": anchor["floor"],
                    "rssi":  _bt_rssi_cache[uuid]["rssi"],
                    "seen":  True,
                }
            else:
                result[anchor["name"]] = {"floor": anchor["floor"], "seen": False}
        return result


# ── GLOBAL SYSTEM INSTANCES ───────────────────────────────────────────────────
vector   = VECTOR()
pdcm     = PDCM()
specter  = SPECTER()
meridian = MERIDIAN()
vigil    = VIGIL()
sentinel = SENTINEL()
corona   = CORONA()
codex    = CODEX()
lens     = LENS(pdcm)
mosaic   = MOSAIC()
nexus    = NEXUS()
oracle_p = ORACLE_PRED()

# ── SYSTEM TICK — called every 30s from palantir loop ────────────────────────
def tick(local_state, oracle_state, pal_state):
    """Run all systems on a unified tick."""
    now = time.time()
    pdcm.record_activity()

    # VECTOR — directional metrics
    for key, val in [
        ("local_cpu", local_state.get("cpu_pct", 0)),
        ("local_ram", local_state.get("ram_pct", 0)),
        ("oracle_cpu", oracle_state.get("cpu_pct", 0)),
        ("oracle_ram", oracle_state.get("ram_pct", 0)),
    ]:
        vector.update(key, val)

    # VIGIL — threat check
    devices   = pal_state.get("devices", {}).get("all", [])
    known_macs = set(
        d.get("mac","") for d in pal_state.get("devices", {}).get("phones", [])
        if d.get("is_known")
    )
    vigil.check_network(devices, known_macs)
    vigil.check_system(local_state, "LOCAL")
    vigil.check_system(oracle_state, "ORACLE")

    # SPECTER — behavior observation
    for d in devices:
        specter.observe(d.get("mac", ""), True)

    # CORONA — ingest news
    corona.ingest_news(pal_state.get("news", []))

    # MOSAIC — observe everything
    mosaic.observe("local", "cpu", local_state.get("cpu_pct", 0))
    mosaic.observe("local", "ram", local_state.get("ram_pct", 0))
    mosaic.observe("oracle", "cpu", oracle_state.get("cpu_pct", 0))
    mosaic.observe("oracle", "ram", oracle_state.get("ram_pct", 0))

    # NEXUS — build relationships
    for proc in (local_state.get("top_procs_cpu") or [])[:3]:
        nexus.add_node(proc[0], proc[0], "process")
        nexus.add_edge("local_cpu", proc[0], weight=proc[1]/100)

    # ORACLE_PRED — predictions
    # (history tracked by SHARD in helix, simplified here)

    # SENTINEL — public IP
    sentinel.get_public_ip()

    # CODEX — record significant events
    if local_state.get("cpu_pct", 0) > 85:
        codex.record("high_cpu", "LOCAL",
                     f"CPU {local_state['cpu_pct']:.0f}%",
                     {"cpu": local_state["cpu_pct"]})

    # BT scan results → MERIDIAN
    bt_data = get_bt_rssi()
    for uuid, info in bt_data.items():
        # Skip anchor devices — they're fixed
        if uuid in BT_ANCHORS:
            continue
        rssi  = info.get("rssi", -100)
        floor = info.get("floor_estimate") or _rssi_to_floor(rssi)
        meridian.record_rssi(uuid, rssi)
        # Also directly set floor from calibrated estimate
        if floor and uuid in meridian._floor_state:
            meridian._floor_state[uuid]["floor"] = floor

    return {
        "vigil_threats": vigil.get_active(),
        "patterns": mosaic.get_patterns(),
        "public_ip": sentinel._last_pub_ip,
        "new_connections": sentinel.get_new_connections(5),
        "focus": lens.get_focus_item(),
        "vectors": {
            k: vector.get_direction(k)
            for k in ["local_cpu", "local_ram", "oracle_cpu", "oracle_ram"]
        },
        "floors": meridian.get_all_floors(),
        "specter": specter.get_all_inferences(),
    }


# Known phone UUIDs — learned from scans
# iPhone UUIDs rotate on macOS but names stay consistent
KNOWN_PHONES = {
    "FD8B3251-FD35-3567-E64C-802F3A451309": "iPhone 13 (2)",
    # AirPods — multiple UUIDs for same device
    "5408A8DE-8945-EA68-924A-285B34A9C6C1": "Caiden AirPods",
    "7E02A5F2-4182-009F-E585-C2431D80357C": "Caiden AirPods",
    "C75FE90D-3D57-A205-9AAD-E061C44F4167": "Caiden AirPods",
}

def get_full_state():
    bt = get_bt_rssi()
    # Build phone locations from BT data
    phone_locations = {}
    for uuid, info in bt.items():
        name = info.get("name", "")
        if uuid in KNOWN_PHONES:
            name = KNOWN_PHONES[uuid]
        # Only surface phones and named devices
        if name and uuid not in BT_ANCHORS:
            floor = info.get("floor_estimate") or _rssi_to_floor(info.get("rssi", -100))
            existing = phone_locations.get(name)
            # Keep the strongest signal reading per named device
            if not existing or info.get("rssi", -100) > existing.get("rssi", -100):
                phone_locations[name] = {
                    "floor": floor,
                    "rssi":  info.get("rssi"),
                    "name":  name,
                    "uuid":  uuid,
                    "ts":    info.get("ts"),
                }

    return {
        "vigil":            vigil.get_all(),
        "vigil_active":     vigil.get_active(),
        "patterns":         mosaic.get_patterns(),
        "public_ip":        sentinel._last_pub_ip,
        "new_connections":  sentinel.get_new_connections(10),
        "vectors": {
            k: vector.get_direction(k)
            for k in ["local_cpu", "local_ram", "oracle_cpu", "oracle_ram"]
        },
        "floors":           meridian.get_all_floors(),
        "phone_locations":  phone_locations,
        "anchors":          get_anchors(),
        "specter": {
            k: {"inferred_type": v.get("inferred_type", "unknown"),
                "seen_count":    v.get("seen_count", 0)}
            for k, v in specter.get_all_inferences().items()
        },
        "nexus":            nexus.get_graph(),
        "codex_recent":     codex.query(limit=10),
        "pdcm_profile":     pdcm.get_profile(),
        "bt_rssi":          bt,
        "bt_device_count":  len(bt),
    }
