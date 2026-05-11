#!/usr/bin/env python3
"""
SYSWATCH V3 — Core Module
System data collection, shared state, smart AI call protocol.
Supports multiple named machines.
"""

import os, time, threading, psutil
from datetime import datetime
from collections import deque

# ── OPTIONAL DEPS ─────────────────────────────────────────────────────────────
try:
    import pynvml
    pynvml.nvmlInit()
    GPU_AVAILABLE = True
    GPU_COUNT     = pynvml.nvmlDeviceGetCount()
except Exception:
    GPU_AVAILABLE = False
    GPU_COUNT     = 0

# ── CONFIG ────────────────────────────────────────────────────────────────────
AI_INTERVAL_BY_TIER = {
    "chill":    300,
    "normal":   120,
    "elevated":  60,
    "hot":       30,
    "critical":  15,
}
DELTA_THRESHOLD     = 8.0
HISTORY_LEN         = 300
VERDICT_HISTORY_MAX = 30
MAX_AI_BACKOFF      = 120

# ── TIER HELPERS ──────────────────────────────────────────────────────────────
def tier_for(pct: float) -> str:
    if pct < 40:  return "chill"
    if pct < 60:  return "normal"
    if pct < 75:  return "elevated"
    if pct < 90:  return "hot"
    return "critical"

def tier_index(t: str) -> int:
    return ["chill","normal","elevated","hot","critical"].index(t)

# ── MACHINE STATE FACTORY ─────────────────────────────────────────────────────
def make_machine_state(name: str, label: str) -> dict:
    """Creates an isolated state dict for one machine."""
    return {
        "name":              name,
        "label":             label,
        "online":            True,
        "last_contact":      time.time(),

        # CPU
        "cpu_pct":           0.0,
        "cpu_per_core":      [],
        "cpu_top":           [],
        "cpu_history":       [],
        "cpu_tier":          "chill",
        "cpu_verdict":       "Initializing…",

        # RAM
        "ram_pct":           0.0,
        "ram_used_gb":       0.0,
        "ram_total_gb":      0.0,
        "ram_top":           [],
        "ram_history":       [],
        "ram_tier":          "chill",
        "ram_verdict":       "Initializing…",

        # Thermals
        "cpu_temps":         [],
        "cpu_temp_max":      0.0,
        "thermal_throttle_risk": False,

        # Battery
        "has_battery":       False,
        "battery_pct":       None,
        "battery_charging":  None,
        "battery_mins_left": None,

        # GPU
        "gpus":              [],
        "gpu_available":     False,

        # Network
        "net_connections":   [],
        "net_io":            None,

        # AI
        "combined_verdict":  "Initializing…",
        "tamagotchi_mood":   "happy",
        "tamagotchi_msg":    "",
        "digest":            "",
        "prediction":        "",
        "last_digest_ts":    0,
        "verdict_history":   [],
        "last_updated":      "—",
        "ai_status":         "waiting",
        "ai_call_count":     0,
        "ai_backoff":        15,
        "last_ai_call_ts":   0,

        # Intelligence
        "active_anomalies":  [],
        "recent_events":     [],
        "correlations":      [],
        "score":             1000,
        "grade":             "S",
        "spike_count":       0,
        "score_history":     [],
        "is_quiet_hour":     False,
        "rep_flags":         [],

        # Session
        "session_start":     time.time(),
        "peak_cpu":          0.0,
        "peak_ram":          0.0,

        # Internal tracking
        "_cpu_hist":         deque(maxlen=HISTORY_LEN),
        "_ram_hist":         deque(maxlen=HISTORY_LEN),
        "_verdict_hist":     deque(maxlen=VERDICT_HISTORY_MAX),
        "_last_called_cpu":  0.0,
        "_last_called_ram":  0.0,
        "_last_called_procs_cpu": [],
        "_last_called_procs_ram": [],
    }

# ── LOCAL DATA COLLECTION ─────────────────────────────────────────────────────
def refresh_local(state: dict):
    """Refresh system metrics for the LOCAL machine."""

    # CPU
    raw_cpu = psutil.cpu_percent(interval=0.5)
    raw_cores = psutil.cpu_percent(percpu=True)
    state["_raw_cpu"]      = raw_cpu
    state["_raw_ram"]      = 0  # set below
    state["cpu_per_core"]  = raw_cores
    state["cpu_tier"]      = tier_for(raw_cpu)
    state["peak_cpu"]      = max(state["peak_cpu"], raw_cpu)
    # cpu_pct updated by collect_loop 6s average, but keep raw for history
    if not state.get("_6s_active"):
        state["cpu_pct"] = raw_cpu

    # RAM
    vm = psutil.virtual_memory()
    state["_raw_ram"]      = vm.percent
    state["ram_used_gb"]   = vm.used  / 1e9
    state["ram_total_gb"]  = vm.total / 1e9
    state["ram_tier"]      = tier_for(vm.percent)
    state["peak_ram"]      = max(state["peak_ram"], vm.percent)
    if not state.get("_6s_active"):
        state["ram_pct"] = vm.percent

    # History
    state["_cpu_hist"].append(round(state["cpu_pct"], 1))
    state["_ram_hist"].append(round(state["ram_pct"], 1))
    state["cpu_history"] = list(state["_cpu_hist"])
    state["ram_history"] = list(state["_ram_hist"])

    # Processes
    procs = []
    for p in psutil.process_iter(["name","cpu_percent","memory_info"]):
        try:
            procs.append(p.info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    state["cpu_top"] = sorted(
        [(p["name"], p["cpu_percent"]) for p in procs
         if p["cpu_percent"] and p["cpu_percent"] > 0],
        key=lambda x: x[1], reverse=True
    )[:6]

    state["ram_top"] = sorted(
        [(p["name"], p["memory_info"].rss / 1e6) for p in procs
         if p.get("memory_info")],
        key=lambda x: x[1], reverse=True
    )[:6]

    # Thermals
    temps = []
    try:
        raw = psutil.sensors_temperatures()
        for chip, entries in (raw or {}).items():
            for e in entries:
                if e.current and e.current > 0:
                    temps.append((e.label or chip, round(e.current, 1)))
    except Exception:
        pass
    state["cpu_temps"]    = temps[:8]
    state["cpu_temp_max"] = max((t for _,t in temps), default=0.0)
    state["thermal_throttle_risk"] = state["cpu_temp_max"] >= 85

    # Battery
    try:
        bat = psutil.sensors_battery()
        if bat:
            state["has_battery"]      = True
            state["battery_pct"]      = round(bat.percent, 1)
            state["battery_charging"] = bat.power_plugged
            mins = bat.secsleft / 60 if bat.secsleft not in (-1,-2) else None
            state["battery_mins_left"] = round(mins) if mins else None
        else:
            state["has_battery"] = False
    except Exception:
        state["has_battery"] = False

    # GPU
    gpus = []
    if GPU_AVAILABLE:
        try:
            for i in range(GPU_COUNT):
                h    = pynvml.nvmlDeviceGetHandleByIndex(i)
                name = pynvml.nvmlDeviceGetName(h)
                if isinstance(name, bytes): name = name.decode()
                util = pynvml.nvmlDeviceGetUtilizationRates(h)
                mem  = pynvml.nvmlDeviceGetMemoryInfo(h)
                temp = pynvml.nvmlDeviceGetTemperature(h, pynvml.NVML_TEMPERATURE_GPU)
                try:    clock = pynvml.nvmlDeviceGetClockInfo(h, pynvml.NVML_CLOCK_GRAPHICS)
                except: clock = 0
                gpus.append({
                    "name": name, "util_pct": util.gpu,
                    "vram_used_mb": round(mem.used/1e6),
                    "vram_total_mb": round(mem.total/1e6),
                    "temp_c": temp, "clock_mhz": clock,
                })
        except Exception:
            pass
    state["gpus"]          = gpus
    state["gpu_available"] = GPU_AVAILABLE

    # Network
    conns = []
    try:
        pid_names = {p.pid: p.info["name"]
                     for p in psutil.process_iter(["name"]) if p.info.get("name")}
        for c in psutil.net_connections(kind="inet"):
            if c.status in ("ESTABLISHED","LISTEN") and c.raddr:
                conns.append({
                    "proc":   pid_names.get(c.pid, "unknown"),
                    "lport":  c.laddr.port if c.laddr else None,
                    "raddr":  f"{c.raddr.ip}:{c.raddr.port}",
                    "status": c.status,
                })
    except Exception:
        pass
    state["net_connections"] = conns[:15]

    # Network I/O bandwidth
    try:
        nio = psutil.net_io_counters()
        state["net_io"] = {"bytes_sent": nio.bytes_sent, "bytes_recv": nio.bytes_recv}
    except Exception:
        state["net_io"] = None

    # Tamagotchi mood
    worst = max(state["cpu_pct"], state["ram_pct"])
    if worst < 30:   state["tamagotchi_mood"] = "happy"
    elif worst < 55: state["tamagotchi_mood"] = "content"
    elif worst < 70: state["tamagotchi_mood"] = "concerned"
    elif worst < 85: state["tamagotchi_mood"] = "sweating"
    else:            state["tamagotchi_mood"] = "screaming"

    state["last_contact"] = time.time()
    state["online"]       = True

# ── SMART AI CALL PROTOCOL ────────────────────────────────────────────────────
def should_call_ai(state: dict) -> tuple[bool, str]:
    now      = time.time()
    cpu      = state["cpu_pct"]
    ram      = state["ram_pct"]
    ct       = state["cpu_tier"]
    rt       = state["ram_tier"]
    worst    = ct if tier_index(ct) >= tier_index(rt) else rt
    min_int  = AI_INTERVAL_BY_TIER[worst]
    elapsed  = now - state["last_ai_call_ts"]

    if elapsed < state["ai_backoff"]:
        return False, "backoff"

    if elapsed < min_int:
        prev_ct = tier_for(state["_last_called_cpu"])
        prev_rt = tier_for(state["_last_called_ram"])
        if tier_index(ct) > tier_index(prev_ct): return True, f"cpu tier → {ct}"
        if tier_index(rt) > tier_index(prev_rt): return True, f"ram tier → {rt}"
        return False, "min_interval"

    if abs(cpu - state["_last_called_cpu"]) >= DELTA_THRESHOLD: return True, "cpu delta"
    if abs(ram - state["_last_called_ram"]) >= DELTA_THRESHOLD: return True, "ram delta"

    procs_cpu = [n for n,_ in state["cpu_top"][:3]]
    procs_ram = [n for n,_ in state["ram_top"][:3]]
    if procs_cpu != state["_last_called_procs_cpu"]: return True, "new cpu proc"
    if procs_ram != state["_last_called_procs_ram"]: return True, "new ram proc"

    if elapsed >= min_int * 3: return True, "staleness"
    return False, "no change"

def record_ai_called(state: dict):
    state["_last_called_cpu"]       = state["cpu_pct"]
    state["_last_called_ram"]       = state["ram_pct"]
    state["_last_called_procs_cpu"] = [n for n,_ in state["cpu_top"][:3]]
    state["_last_called_procs_ram"] = [n for n,_ in state["ram_top"][:3]]
    state["last_ai_call_ts"]        = time.time()
    state["ai_call_count"]         += 1

# ── VERDICT HISTORY ───────────────────────────────────────────────────────────
def push_verdict(state: dict, verdict: str):
    entry = {
        "ts":      datetime.now().strftime("%H:%M:%S"),
        "verdict": verdict,
        "cpu_pct": state["cpu_pct"],
        "ram_pct": state["ram_pct"],
        "tier":    state["cpu_tier"],
        "score":   state.get("score", 1000),
    }
    state["_verdict_hist"].append(entry)
    state["verdict_history"] = list(state["_verdict_hist"])

# ── PROMPT BUILDERS ───────────────────────────────────────────────────────────
def _base_context(state: dict) -> str:
    cpu_list = ", ".join(f"{n}({v:.1f}%)" for n,v in state["cpu_top"]) or "none"
    ram_list = ", ".join(f"{n}({v:.0f}MB)" for n,v in state["ram_top"]) or "none"

    gpu_info = ""
    if state["gpus"]:
        g = state["gpus"][0]
        gpu_info = f"\nGPU: {g['name']} {g['util_pct']}% util, {g['vram_used_mb']}/{g['vram_total_mb']}MB VRAM, {g['temp_c']}°C"

    temp_info = ""
    if state["cpu_temp_max"] > 0:
        temp_info = f"\nTemp: {state['cpu_temp_max']}°C{'  ⚠ THROTTLE RISK' if state['thermal_throttle_risk'] else ''}"

    bat_info = ""
    if state["has_battery"] and state["battery_pct"] is not None:
        status = "charging" if state["battery_charging"] else "on battery"
        mins   = f", ~{state['battery_mins_left']}min left" if state["battery_mins_left"] else ""
        bat_info = f"\nBattery: {state['battery_pct']}% ({status}{mins})"

    trend = ""
    if len(state["cpu_history"]) >= 10:
        slope = state["cpu_history"][-1] - state["cpu_history"][-10]
        trend = f" [{'↑' if slope>3 else '↓' if slope<-3 else '→'}{abs(slope):.1f}% / 10s]"

    intel = ""
    if state.get("active_anomalies"):
        intel += "\nACTIVE ANOMALIES: " + "; ".join(
            f"{a['proc']} {a['metric']} spike {a['peak']:.0f} for {a['duration_s']}s"
            for a in state["active_anomalies"][:3]
        )
    if state.get("rep_flags"):
        intel += "\nREP FLAGS: " + "; ".join(state["rep_flags"][:3])
    if state.get("correlations"):
        intel += "\nCAUSALITY: " + "; ".join(c["desc"] for c in state["correlations"][:2])

    hist = ""
    if state["verdict_history"]:
        last3 = state["verdict_history"][-3:]
        hist  = "\nRECENT: " + " | ".join(
            f"[{v['ts']}] CPU{v['cpu_pct']:.0f}% RAM{v['ram_pct']:.0f}%"
            for v in last3
        )

    return (
        f"Machine: {state['label']}\n"
        f"CPU: {state['cpu_pct']:.1f}%{trend} | {cpu_list}\n"
        f"RAM: {state['ram_pct']:.1f}% ({state['ram_used_gb']:.1f}/{state['ram_total_gb']:.1f}GB) | {ram_list}"
        f"{gpu_info}{temp_info}{bat_info}{intel}{hist}"
    )

def _cypher_context(state: dict) -> str:
    """Build Cypher infrastructure context for oracle AI prompts."""
    procs = state.get("pm2_processes", [])
    if not procs:
        return ""
    lines = ["\nCYPHER INFRASTRUCTURE (PM2):"]
    for p in procs:
        status = "✓" if p["status"] == "online" else "✕ DOWN"
        lines.append(f"  {p['name']}: {status} | restarts: {p['restarts']} | mem: {p['mem_mb']}MB")
    alerts = state.get("pm2_alerts", [])
    if alerts:
        lines.append("ALERTS: " + "; ".join(alerts))
    cypher_up = state.get("cypher_online", False)
    lines.append(f"Cypher overall: {'ONLINE' if cypher_up else 'DEGRADED'}")
    return "\n".join(lines)

def build_verdict_prompt(state: dict) -> str:
    quiet_note = "\nNOTE: This is typically a quiet hour — lower activity is expected." \
                 if state.get("is_quiet_hour") else ""
    bat_note   = ""
    if state["has_battery"] and not state["battery_charging"] and state["battery_mins_left"]:
        bat_note = f"\nBattery: {state['battery_mins_left']} min left — factor into tone."

    cypher_note = _cypher_context(state) if state.get("name") == "oracle" else ""
    server_note = "\nThis is a Linux cloud server running Cypher's backend infrastructure." \
                  if state.get("name") == "oracle" else \
                  "\nThis is a local Mac developer machine. Do NOT reference Cypher services or PM2."

    verdict_instructions = """Write THREE outputs (1-2 sentences, plain text, no markdown):
CPU VERDICT: Vivid, specific, name actual apps or services. Reference anomaly/reputation data if present.
RAM VERDICT: Same for RAM. Mention notable memory consumers.
TAMAGOTCHI: One sentence in first person as the creature, mood-appropriate.""" if state.get("name") != "oracle" else \
"""Write THREE outputs (1-2 sentences, plain text, no markdown):
CPU VERDICT: Vivid, specific, name actual apps or Cypher services. Reference anomaly/reputation data if present.
RAM VERDICT: Same for RAM. Mention Cypher service memory if notable.
TAMAGOTCHI: One sentence in first person as the creature, mood-appropriate. If Cypher is degraded, panic accordingly."""

    return f"""You are a sarcastic but genuinely helpful AI system monitor. Speak as the TAMA-9000 creature living inside this machine.{quiet_note}{bat_note}{server_note}

{_base_context(state)}{cypher_note}

Current mood: {state['tamagotchi_mood']}
Session score: {state.get('score', 1000)}/1000 (Grade {state.get('grade','S')})

{verdict_instructions}

Format EXACTLY:
CPU VERDICT: <sentence>
RAM VERDICT: <sentence>
TAMAGOTCHI: <sentence>"""

def build_digest_prompt(state: dict) -> str:
    hist  = state["cpu_history"]
    rhist = state["ram_history"]
    mins  = max(1, len(hist) // 60)
    cpu_avg  = sum(hist) / max(1, len(hist))
    ram_avg  = sum(rhist) / max(1, len(rhist))

    anomaly_summary = ""
    if state.get("recent_events"):
        ev = state["recent_events"][-5:]
        anomaly_summary = "\nRecent anomalies: " + "; ".join(
            f"{e['proc_name']} {e['metric']} spike ({e['peak_val']:.0f}, {e.get('duration_s',0):.0f}s)"
            for e in ev
        )

    return f"""System digest for {state['label']}. Last ~{mins} minutes.
CPU avg {cpu_avg:.1f}% peak {max(hist, default=0):.1f}% | top: {", ".join(n for n,_ in state["cpu_top"][:3])}
RAM avg {ram_avg:.1f}% | top: {", ".join(n for n,_ in state["ram_top"][:3])}
Score: {state.get('score',1000)}/1000{anomaly_summary}

Write a 2-3 sentence plain-text system story. Be specific about apps and events."""

def build_prediction_prompt(state: dict) -> str:
    hist = state["cpu_history"][-60:] if len(state["cpu_history"]) >= 60 else state["cpu_history"]
    if len(hist) < 2: return ""
    return f"""Predictive analysis for {state['label']}.
CPU last {len(hist)}s: {hist[0]:.1f}% → {hist[-1]:.1f}%
Samples: {','.join(str(x) for x in hist[::10])}
RAM: {state['ram_pct']:.1f}%

One plain-text sentence: predict what happens in 2-3 minutes. Be specific. If stable say so."""

def build_nl_query_prompt(state: dict, question: str) -> str:
    return f"""Helpful AI system monitor. Answer concisely.

{_base_context(state)}
Score: {state.get('score',1000)}/1000

Question: {question}

2-4 sentences, plain text, actionable."""

def build_postmortem_prompt(state: dict, cpu_snap: list, ram_snap: list, top_snap: str) -> str:
    return f"""Post-mortem for {state['label']}.
60s ago: CPU={cpu_snap[-1] if cpu_snap else '?'}% RAM={ram_snap[-1] if ram_snap else '?'}%
Now: CPU={state['cpu_pct']:.1f}% RAM={state['ram_pct']:.1f}%
Trend: {','.join(str(x) for x in (cpu_snap[::6] if cpu_snap else []))}
Processes at spike: {top_snap}

2-3 sentences: what happened, why, is it resolved?"""

def build_spotlight_prompt(state: dict, proc: str, cpu: float, ram: float) -> str:
    rep_info = ""
    if state.get("rep_flags"):
        matching = [f for f in state["rep_flags"] if proc in f]
        if matching: rep_info = f"\nReputation: {matching[0]}"

    return f"""System monitor explaining a process.
Machine: {state['label']}
Process: {proc} | CPU: {cpu:.1f}% | RAM: {ram:.0f}MB
System: CPU={state['cpu_pct']:.1f}% RAM={state['ram_pct']:.1f}%{rep_info}

3-4 sentences: what is it, is this usage normal, safe to kill?"""

# ── LOCAL DATA LOOP ───────────────────────────────────────────────────────────
def local_data_loop(state: dict, intel_bundle: dict = None):
    """Runs in a thread, refreshes local state every second."""
    tick = 0
    while True:
        try:
            refresh_local(state)
            if intel_bundle:
                _run_intelligence(state, intel_bundle, tick)
            tick += 1
        except Exception:
            pass
        time.sleep(1)

def _run_intelligence(state: dict, bundle: dict, tick: int):
    """Feed data into intelligence engines."""
    rep      = bundle.get("reputation")
    anomaly  = bundle.get("anomaly")
    causality= bundle.get("causality")
    quiet    = bundle.get("quiet")
    scoring  = bundle.get("scoring")

    if not rep: return

    # Update reputation baselines
    for proc, cpu in state["cpu_top"]:
        rep.update(proc, "cpu", cpu)
    for proc, ram in state["ram_top"]:
        rep.update(proc, "ram", ram)

    # Check anomalies
    for proc, cpu in state["cpu_top"]:
        anomaly.check(proc, "cpu", cpu)
    for proc, ram in state["ram_top"]:
        anomaly.check(proc, "ram", ram)

    # Causality
    active_procs = [n for n,_ in state["cpu_top"][:5]]
    causality.record(state["cpu_pct"], state["ram_pct"], active_procs)

    # Quiet period
    quiet.record(state["cpu_pct"])

    # Update state with intelligence outputs
    state["active_anomalies"] = anomaly.get_active_anomalies()
    state["recent_events"]    = anomaly.get_recent_events(10)
    state["correlations"]     = causality.get_correlations()
    state["is_quiet_hour"]    = quiet.is_quiet_hour()

    # Reputation flags
    flags = []
    for proc, cpu in state["cpu_top"][:4]:
        d = rep.describe(proc, "cpu", cpu)
        if d: flags.append(d)
    for proc, ram in state["ram_top"][:4]:
        d = rep.describe(proc, "ram", ram)
        if d: flags.append(d)
    state["rep_flags"] = flags

    # Score
    s = scoring.compute(
        state["peak_cpu"], state["peak_ram"],
        state["thermal_throttle_risk"],
        len(state["active_anomalies"])
    )
    state["score"]       = s["score"]
    state["grade"]       = s["grade"]
    state["spike_count"] = s["spikes"]

    # Flush to DB every 60 ticks
    if tick % 60 == 0:
        rep.flush_to_db()
        scoring.flush(s["score"], state["peak_cpu"], state["peak_ram"])
        state["score_history"] = scoring.get_history()

# ── COLLECTION LOOP ────────────────────────────────────────────────────────────
def collect_loop(state: dict, bundle: dict, post_tick_fn=None):
    """Main 1Hz collection loop — averages CPU/RAM over 6s before updating display."""
    tick      = 0
    _cpu_buf  = []
    _ram_buf  = []
    _core_buf = []
    state["_6s_active"] = True

    while True:
        try:
            # Collect raw every second into buffer
            refresh_local(state)
            _cpu_buf.append(state.get("_raw_cpu", state.get("cpu_pct", 0)))
            _ram_buf.append(state.get("_raw_ram", state.get("ram_pct", 0)))
            cores = state.get("cpu_per_core", [])
            if cores:
                _core_buf.append(cores)

            # Every 6 seconds, push averaged values to display state
            if len(_cpu_buf) >= 6:
                state["cpu_pct"]      = round(sum(_cpu_buf) / len(_cpu_buf), 1)
                state["ram_pct"]      = round(sum(_ram_buf) / len(_ram_buf), 1)
                if _core_buf:
                    n = len(_core_buf[0])
                    state["cpu_per_core"] = [
                        round(sum(frame[i] for frame in _core_buf if i < len(frame)) / len(_core_buf), 1)
                        for i in range(n)
                    ]
                _cpu_buf.clear()
                _ram_buf.clear()
                _core_buf.clear()

            _run_intelligence(state, bundle, tick)
            if post_tick_fn:
                post_tick_fn(state, bundle, tick)
        except Exception:
            pass
        tick += 1
        time.sleep(1)
