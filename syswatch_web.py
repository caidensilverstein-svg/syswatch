#!/usr/bin/env python3
"""
SYSWATCH V5 — Web Dashboard
Usage:
  python3 syswatch_web.py --key YOUR_GROQ_KEY
  python3 syswatch_web.py --key YOUR_GROQ_KEY --oracle http://IP:8766
  python3 syswatch_web.py --quiet
Requires: pip install psutil groq rich
"""

import os, sys, json, time, threading, argparse, re, subprocess
from datetime import datetime
from http.server import ThreadingHTTPServer as HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen
from groq import Groq

sys.path.insert(0, os.path.dirname(__file__))
import core
from intelligence import (
    init_db as init_intel_db, ProcessReputation, AnomalyTracker,
    CausalityEngine, QuietPeriodTracker, ScoringEngine
)
from helix import HELIX
import palantir
import systems

MODEL   = "llama-3.1-8b-instant"
PORT    = 8765
PRED_IV = 180

client     = None
quiet      = False
oracle_url = None

# ── MACHINE STATES ─────────────────────────────────────────────────────────
local_state  = core.make_machine_state("local",  "LOCAL MAC")
oracle_state = core.make_machine_state("oracle", "ORACLE CLOUD")
# Initialize AI rate-limit tracking keys
for _s in (local_state, oracle_state):
    _s.setdefault("_ai_last_cpu", -999)
    _s.setdefault("_ai_last_ram", -999)
    _s.setdefault("_ai_last_tier", "")
    _s.setdefault("_ai_last_incident_count", 0)
    _s.setdefault("_last_pred_ts", 0)
    _s.setdefault("_last_digest_ts", 0)
oracle_state["online"] = False

# ── INTELLIGENCE BUNDLES ────────────────────────────────────────────────────
init_intel_db()
def _make_intel(name):
    rep = ProcessReputation(name)
    anom = None
    bundle = {
        "reputation": rep,
        "anomaly":    None,
        "causality":  CausalityEngine(),
        "quiet":      QuietPeriodTracker(name),
        "scoring":    ScoringEngine(name),
    }
    bundle["anomaly"] = AnomalyTracker(name, rep)
    return bundle

local_intel  = _make_intel("local")
oracle_intel = _make_intel("oracle")

# ── HELIX INSTANCES ─────────────────────────────────────────────────────────
local_helix  = HELIX("local")
oracle_helix = HELIX("oracle")

# ── SMOOTHED STATE FOR AI ──────────────────────────────────────────────────
# Raw 1s data goes to display; 30s rolling average feeds AI prompts
_SMOOTH_WIN = 30
_smooth_bufs = {
    "local":  {"cpu": [], "ram": []},
    "oracle": {"cpu": [], "ram": []},
}

def _apply_smooth(state: dict, machine: str):
    """Replace cpu_pct/ram_pct in state with 30s rolling averages for AI calls."""
    bufs = _smooth_bufs[machine]
    bufs["cpu"].append(state.get("cpu_pct", 0))
    bufs["ram"].append(state.get("ram_pct", 0))
    if len(bufs["cpu"]) > _SMOOTH_WIN: bufs["cpu"].pop(0)
    if len(bufs["ram"]) > _SMOOTH_WIN: bufs["ram"].pop(0)
    state["_smooth_cpu"] = round(sum(bufs["cpu"]) / len(bufs["cpu"]), 1)
    state["_smooth_ram"] = round(sum(bufs["ram"]) / len(bufs["ram"]), 1)

def _smoothed_state(state: dict) -> dict:
    """Return a shallow copy of state with smoothed cpu/ram for AI prompts."""
    s = dict(state)
    if "_smooth_cpu" in state:
        s["cpu_pct"] = state["_smooth_cpu"]
        s["ram_pct"] = state["_smooth_ram"]
    return s

# ── SNAP / TERMINAL BUFFERS ─────────────────────────────────────────────────
_local_snap  = {"cpu": [], "ram": [], "top": ""}
_oracle_snap = {"cpu": [], "ram": [], "top": ""}
_snap_lock   = threading.Lock()
_terminal_buf = {"local": [], "oracle": []}
_terminal_lock = threading.Lock()

# ── AI ENGINE ───────────────────────────────────────────────────────────────
def _call_groq(prompt, max_tokens=220, temp=0.85):
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role":"user","content": prompt}],
        max_tokens=max_tokens,
        temperature=temp,
    )
    return resp.choices[0].message.content.strip()

def _extract(text, label):
    m = re.search(rf"(?im)^{re.escape(label)}\s*[:\-]\s*(.+)", text)
    if m: return m.group(1).strip()
    m = re.search(rf"(?i){re.escape(label)}\s*[:\-]\s*(.+?)(?=\n[A-Z]{{3,}}[:\-]|$)", text, re.DOTALL)
    if m: return m.group(1).strip().split("\n")[0].strip()
    return ""

def fetch_verdicts(state, helix_inst):
    state["ai_status"] = "fetching"
    try:
        text  = _call_groq(core.build_verdict_prompt(_smoothed_state(state)), max_tokens=280)
        cpu_v = _extract(text, "CPU VERDICT")
        ram_v = _extract(text, "RAM VERDICT")
        tama  = _extract(text, "TAMAGOTCHI")
        conf  = _extract(text, "CONFIDENCE")
        if not cpu_v and not ram_v:
            cpu_v = text.strip().split("\n")[0][:120]
        if cpu_v: state["cpu_verdict"]    = cpu_v
        if ram_v: state["ram_verdict"]    = ram_v
        if tama:  state["tamagotchi_msg"] = tama
        if conf:  state["ai_confidence"]  = conf
        core.push_verdict(state, f"{cpu_v} | {ram_v}")
        state["last_updated"] = datetime.now().strftime("%H:%M:%S")
        state["ai_status"]    = "ready"
        state["ai_backoff"]   = 15
        core.record_ai_called(state)
    except Exception as e:
        state["cpu_verdict"] = f"AI error: {e}"
        state["ai_status"]   = "error"
        state["ai_backoff"]  = min(core.MAX_AI_BACKOFF, state["ai_backoff"] * 2)

def maybe_fetch_prediction(state):
    if time.time() - state.get("_last_pred_ts", 0) < PRED_IV: return
    try:
        p = core.build_prediction_prompt(_smoothed_state(state))
        if p:
            state["prediction"]    = _call_groq(p, max_tokens=80, temp=0.7)
            state["_last_pred_ts"] = time.time()
    except: pass

def maybe_fetch_digest(state):
    if time.time() - state["last_digest_ts"] < 300: return
    try:
        p = core.build_digest_prompt(_smoothed_state(state))
        if p:
            state["digest"]        = _call_groq(p, max_tokens=160, temp=0.7)
            state["last_digest_ts"] = time.time()
    except: pass

def fetch_narrative(local, oracle):
    """Cross-machine correlation narrative — fires every 5 min."""
    key = "_last_narrative_ts"
    if time.time() - local.get(key, 0) < 300: return
    try:
        lc = f"LOCAL MAC: CPU {local['cpu_pct']:.1f}% RAM {local['ram_pct']:.1f}%"
        oc = f"ORACLE: CPU {oracle['cpu_pct']:.1f}% RAM {oracle['ram_pct']:.1f}%"
        pm2 = ", ".join(
            f"{p['name']}({'UP' if p['status']=='online' else 'DOWN'})"
            for p in oracle.get("pm2_processes",[])
        )
        prompt = f"""System narrative for a two-machine stack.
{lc} | top procs: {', '.join(n for n,_ in local.get('cpu_top',[])[:3])}
{oc} | PM2: {pm2}
Score LOCAL {local.get('score',1000)}/1000  ORACLE {oracle.get('score',1000)}/1000

Write 2-3 sentences in plain text describing the overall state of BOTH machines together.
Note any cross-machine patterns, service health, and what the operator should be aware of.
Be specific and direct. No markdown."""
        result = _call_groq(prompt, max_tokens=180, temp=0.7)
        local["cross_narrative"]  = result
        oracle["cross_narrative"] = result
        local[key] = time.time()
    except: pass

def fetch_root_cause(state):
    """Root cause hypothesis with confidence ranking."""
    if time.time() - state.get("_last_rca_ts", 0) < 120: return
    if not state.get("active_anomalies"): return
    try:
        s = _smoothed_state(state)
        anoms = s.get("active_anomalies", [])
        anom_str = "; ".join(f"{a['proc']} {a['metric']} {a['peak']:.0f} for {a['duration_s']}s"
                              for a in anoms[:3])
        prompt = f"""Root cause analysis for {state['label']}.
Active anomalies: {anom_str}
CPU: {state['cpu_pct']:.1f}% RAM: {state['ram_pct']:.1f}%
Top CPU: {', '.join(n for n,_ in state.get('cpu_top',[])[:4])}
Top RAM: {', '.join(n for n,_ in state.get('ram_top',[])[:4])}

List 2-3 root cause hypotheses, each on its own line, format:
HYPOTHESIS: <cause> | CONFIDENCE: <pct>% | ACTION: <one word action>
Plain text only."""
        result = _call_groq(prompt, max_tokens=200, temp=0.6)
        state["root_cause"] = result
        state["_last_rca_ts"] = time.time()
    except: pass

def _has_meaningful_change(state):
    """Returns True only if something actually changed enough to warrant an AI call."""
    now_cpu = state.get("cpu_pct", 0)
    now_ram = state.get("ram_pct", 0)
    last_cpu = state.get("_ai_last_cpu", -999)
    last_ram = state.get("_ai_last_ram", -999)
    last_tier = state.get("_ai_last_tier", "")
    curr_tier = state.get("cpu_tier", "")
    # Only call AI if: tier changed, OR >8% CPU swing, OR >10% RAM swing
    tier_changed = curr_tier != last_tier
    cpu_swing    = abs(now_cpu - last_cpu) > 8
    ram_swing    = abs(now_ram - last_ram) > 10
    new_incident = len(state.get("incidents", [])) != state.get("_ai_last_incident_count", 0)
    return tier_changed or cpu_swing or ram_swing or new_incident

def ai_loop_for(state, helix_inst, intel):
    MIN_INTERVAL = 90   # never call AI more than once per 90s per machine
    last_call    = 0
    while True:
        try:
            if not quiet and state.get("online", True):
                now    = time.time()
                forced = helix_inst.pop_ai_forced()
                meaningful = _has_meaningful_change(state)
                elapsed    = now - last_call
                should_call = (forced or meaningful) and elapsed >= MIN_INTERVAL
                if should_call:
                    fetch_verdicts(state, helix_inst)
                    fetch_root_cause(state)
                    # Record what AI last saw
                    state["_ai_last_cpu"]            = state.get("cpu_pct", 0)
                    state["_ai_last_ram"]            = state.get("ram_pct", 0)
                    state["_ai_last_tier"]           = state.get("cpu_tier", "")
                    state["_ai_last_incident_count"] = len(state.get("incidents", []))
                    last_call = now
                    # Prediction only every 10 minutes
                    if now - state.get("_last_pred_ts", 0) > 600:
                        maybe_fetch_prediction(state)
                # Digest only every 20 minutes
                if now - state.get("_last_digest_ts", 0) > 1200:
                    maybe_fetch_digest(state)
                    state["_last_digest_ts"] = now
        except Exception:
            pass
        time.sleep(5)

def narrative_loop():
    while True:
        if not quiet:
            try: fetch_narrative(local_state, oracle_state)
            except: pass
        time.sleep(900)  # 15 minutes

def handle_nl_query(state, question):
    if quiet or not client: return "AI disabled."
    try: return _call_groq(core.build_nl_query_prompt(state, question), max_tokens=200, temp=0.7)
    except Exception as e: return f"AI error: {e}"

def handle_postmortem(state, snap):
    if quiet or not client: return "AI disabled."
    try: return _call_groq(
        core.build_postmortem_prompt(state, snap["cpu"], snap["ram"], snap["top"]),
        max_tokens=200, temp=0.7)
    except Exception as e: return f"AI error: {e}"

def handle_spotlight(state, proc, cpu, ram):
    if quiet or not client: return "AI disabled."
    try: return _call_groq(
        core.build_spotlight_prompt(state, proc, cpu, ram),
        max_tokens=200, temp=0.7)
    except Exception as e: return f"AI error: {e}"

def handle_incident_query(state, ts_str):
    if quiet or not client: return "AI disabled."
    try:
        ts = float(ts_str)
        machine_key = "local" if "LOCAL" in state.get("label","").upper() else "oracle"
        helix_inst  = local_helix if machine_key == "local" else oracle_helix
        chain = helix_inst.trace.reconstruct_chain(ts)
        chain_str = "; ".join(
            f"{datetime.fromtimestamp(e['ts']).strftime('%H:%M:%S')} {e['type']} {e['proc']} {e['val']:.0f}"
            for e in chain[-10:]
        )
        prompt = f"""Incident analysis for {state['label']} around {datetime.fromtimestamp(ts).strftime('%H:%M:%S')}.
Event chain leading up to incident:
{chain_str or 'No chain data available.'}
CPU: {state['cpu_pct']:.1f}% RAM: {state['ram_pct']:.1f}%

Explain what happened in 2-3 sentences. Identify the root cause and what resolved or could resolve it."""
        return _call_groq(prompt, max_tokens=200, temp=0.7)
    except Exception as e: return f"Error: {e}"

def handle_execute_sre(machine, action_id):
    helix_inst = local_helix if machine == "local" else oracle_helix
    if machine == "oracle" and oracle_url:
        def oracle_exec(cmd):
            try:
                with urlopen(f"{oracle_url}/exec?cmd={cmd}", timeout=10) as r:
                    return r.read().decode()
            except Exception as e:
                return f"Remote exec error: {e}"
        return helix_inst.sre.execute(action_id, oracle_exec_fn=oracle_exec)
    return helix_inst.sre.execute(action_id)

# ── ORACLE POLLER ────────────────────────────────────────────────────────────
def oracle_poll_loop():
    tick = 0
    while True:
        if oracle_url:
            try:
                with urlopen(oracle_url + "/metrics", timeout=4) as r:
                    data = json.loads(r.read().decode())
                for key in ["cpu_pct","cpu_per_core","cpu_top","cpu_history",
                            "ram_pct","ram_used_gb","ram_total_gb","ram_top","ram_history",
                            "cpu_temps","cpu_temp_max","thermal_throttle_risk",
                            "has_battery","battery_pct","battery_charging","battery_mins_left",
                            "gpus","gpu_available","net_connections","net_io","peak_cpu","peak_ram",
                            "pm2_processes","pm2_available","cypher_online","critical_down",
                            "total_restarts","pm2_alerts","pm2_last_updated","uptime_s"]:
                    if key in data:
                        oracle_state[key] = data[key]
                _apply_smooth(oracle_state, "oracle")
                oracle_state["cpu_tier"]   = core.tier_for(oracle_state["cpu_pct"])
                oracle_state["ram_tier"]   = core.tier_for(oracle_state["ram_pct"])
                oracle_state["online"]     = True
                oracle_state["last_contact"] = time.time()
                worst = max(oracle_state["cpu_pct"], oracle_state["ram_pct"])
                if worst < 30:   oracle_state["tamagotchi_mood"] = "happy"
                elif worst < 55: oracle_state["tamagotchi_mood"] = "content"
                elif worst < 70: oracle_state["tamagotchi_mood"] = "concerned"
                elif worst < 85: oracle_state["tamagotchi_mood"] = "sweating"
                else:            oracle_state["tamagotchi_mood"] = "screaming"
                core._run_intelligence(oracle_state, oracle_intel, tick)
                oracle_helix.process_tick(oracle_state)
                oracle_state.update(oracle_helix.get_export())
                tick += 1
            except Exception:
                if time.time() - oracle_state["last_contact"] > 10:
                    oracle_state["online"] = False
        time.sleep(1)

def snapshot_loop():
    while True:
        with _snap_lock:
            _local_snap["cpu"]  = list(local_state["cpu_history"])
            _local_snap["ram"]  = list(local_state["ram_history"])
            _local_snap["top"]  = ", ".join(f"{n}({v:.1f}%)" for n,v in local_state["cpu_top"][:4])
            _oracle_snap["cpu"] = list(oracle_state["cpu_history"])
            _oracle_snap["ram"] = list(oracle_state["ram_history"])
            _oracle_snap["top"] = ", ".join(f"{n}({v:.1f}%)" for n,v in oracle_state["cpu_top"][:4])
        time.sleep(5)

def serialise(state):
    skip = {"_cpu_hist","_ram_hist","_verdict_hist",
            "_last_called_cpu","_last_called_ram",
            "_last_called_procs_cpu","_last_called_procs_ram",
            "_last_pred_ts","_last_rca_ts","_last_narrative_ts",
            "_smooth_cpu","_smooth_ram"}
    return {k: v for k, v in state.items() if k not in skip}

# ── HTML ─────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SYSWATCH V5</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syncopate:wght@400;700&family=DM+Mono:ital,wght@0,300;0,400;0,500;1,300&display=swap" rel="stylesheet" media="print" onload="this.media='all'">
<style>
:root{--bg:#05060a;--surface:#080a10;--card:#0a0d15;--border:#12192a;--border2:#1a2540;
--gold:#c9a84c;--gold2:#e8c97a;--gold-dim:#6b5620;--red:#e8442a;--amber:#e87d2a;
--green:#2ae87a;--blue:#3a8fd4;--purple:#9b6dff;--text:#c8d4e8;--muted:#8a9db9;
--muted2:#4a5e7a;--teal:#2addc9;--pink:#e84a8a;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:'DM Mono',monospace;font-size:13px;line-height:1.6;overflow-x:hidden;}
::-webkit-scrollbar{width:4px;} ::-webkit-scrollbar-track{background:var(--bg);} ::-webkit-scrollbar-thumb{background:var(--border2);}
.scan-line{content:'';position:fixed;inset:0;z-index:997;pointer-events:none;
background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,.03) 2px,rgba(0,0,0,.03) 4px);}

/* HEADER */
header{display:flex;align-items:center;justify-content:space-between;padding:12px 24px;
border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100;background:var(--bg);}
header::after{content:'';position:absolute;bottom:-1px;left:0;width:120px;height:1px;
background:linear-gradient(90deg,var(--gold),transparent);}
.brand{font-family:'Syncopate',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.3em;color:var(--gold);}
.brand span{color:var(--muted);font-size:.7rem;}
.header-center{display:flex;gap:14px;align-items:center;}
.header-right{display:flex;gap:10px;align-items:center;}
#freeze-btn{background:transparent;border:1px solid var(--border2);color:var(--muted);
padding:4px 12px;font-family:'DM Mono',monospace;font-size:.72rem;letter-spacing:.1em;cursor:pointer;}
#freeze-btn.active{border-color:var(--amber);color:var(--amber);}
#freeze-btn:hover{border-color:var(--gold);color:var(--gold);}
.kbd-hint{font-size:.68rem;color:var(--muted2);letter-spacing:.05em;}

/* PRIORITY FEED */
#priority-feed{background:var(--surface);border-bottom:1px solid var(--border);
padding:7px 24px;display:flex;gap:10px;align-items:center;min-height:34px;overflow-x:auto;}
.feed-label{font-size:.68rem;letter-spacing:.15em;color:var(--muted2);white-space:nowrap;}
.feed-item{font-size:.72rem;padding:3px 10px;border-radius:2px;white-space:nowrap;animation:fadeSlide .4s ease;}
.feed-item.critical{background:rgba(232,68,42,.12);color:var(--red);border:1px solid rgba(232,68,42,.3);}
.feed-item.urgent{background:rgba(232,125,42,.1);color:var(--amber);border:1px solid rgba(232,125,42,.25);}
.feed-item.elevated{background:rgba(58,143,212,.08);color:var(--blue);border:1px solid rgba(58,143,212,.2);}
@keyframes fadeSlide{from{opacity:0;transform:translateX(-8px)}to{opacity:1;transform:none}}

/* COMMAND BAR */
#cmdbar{background:var(--surface);border-bottom:1px solid var(--border);padding:10px 24px;display:flex;gap:10px;}
#cmd-input{flex:1;background:transparent;border:1px solid var(--border2);color:var(--text);
padding:7px 12px;font-family:'DM Mono',monospace;font-size:.8rem;outline:none;}
#cmd-input:focus{border-color:var(--gold);}
#cmd-input::placeholder{color:var(--muted2);}
.cmd-machine-sel{background:var(--card);border:1px solid var(--border2);color:var(--muted);
padding:5px 10px;font-family:'DM Mono',monospace;font-size:.75rem;}
.cmd-btn{background:transparent;border:1px solid var(--border2);color:var(--muted);
padding:5px 14px;font-family:'DM Mono',monospace;font-size:.75rem;cursor:pointer;letter-spacing:.05em;}
.cmd-btn:hover{border-color:var(--gold);color:var(--gold);}
#cmd-resp{font-size:.76rem;color:var(--teal);margin-top:6px;display:none;padding:6px 12px;
border-left:2px solid var(--teal);background:rgba(42,221,201,.05);}

/* NARRATIVE BAR */
#narrative-bar{background:rgba(155,109,255,.04);border-bottom:1px solid rgba(155,109,255,.15);
padding:8px 24px;font-size:.76rem;color:var(--purple);min-height:28px;line-height:1.6;}

/* MAIN GRID */
.main-grid{display:grid;grid-template-columns:1fr 1fr;gap:0;min-height:calc(100vh - 180px);}
.machine-col{padding:20px 24px;border-right:1px solid var(--border);overflow-y:auto;}
.machine-col:last-child{border-right:none;}
.machine-col.focused{box-shadow:inset 0 0 0 1px rgba(201,168,76,.2);}

/* STATUS ROW */
.status-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;}
.machine-label{font-family:'Syncopate',sans-serif;font-size:.85rem;letter-spacing:.2em;}
.status-badge{font-size:.65rem;letter-spacing:.12em;padding:3px 8px;font-weight:700;}
.status-badge.online{color:var(--green);border:1px solid rgba(42,232,122,.3);}
.status-badge.offline{color:var(--muted2);border:1px solid var(--border2);}
.meta-row{font-size:.7rem;color:var(--muted2);letter-spacing:.04em;margin-bottom:14px;}

/* GAUGES */
.gauge-section{margin-bottom:18px;}
.gauge-row{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:4px;}
.gauge-label{font-size:.75rem;letter-spacing:.08em;color:var(--muted);}
.gauge-pct{font-size:2rem;font-weight:400;letter-spacing:-.02em;font-family:'Syncopate',sans-serif;}
.gauge-tier{font-size:.62rem;letter-spacing:.12em;margin-left:8px;padding:2px 7px;}
.gauge-bar-wrap{height:4px;background:var(--border);position:relative;margin-bottom:4px;}
.gauge-bar{height:100%;transition:width .6s ease;}
.gauge-verdict{font-size:.73rem;color:var(--muted);line-height:1.6;min-height:22px;}
.gauge-forecast{font-size:.68rem;color:var(--muted2);margin-top:3px;}
canvas.spark{display:block;width:100%;height:48px;}

/* SCORE */
.score-section{display:flex;align-items:center;gap:14px;margin-bottom:16px;padding:10px 16px;
background:var(--surface);border:1px solid var(--border);}
.score-num{font-size:2rem;font-family:'Syncopate',sans-serif;font-weight:700;}
.score-grade{font-size:1.1rem;font-family:'Syncopate',sans-serif;margin-left:6px;opacity:.7;}
.score-label{font-size:.65rem;letter-spacing:.1em;color:var(--muted2);}
.score-spark-wrap{flex:1;height:36px;min-width:80px;}

/* SECTION TITLES */
.section-title{font-size:.68rem;letter-spacing:.16em;color:var(--muted2);
margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);}

/* CORE HEATMAP */
.core-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(36px,1fr));gap:4px;margin-bottom:14px;}
.core-cell{height:28px;border-radius:3px;display:flex;align-items:center;justify-content:center;
font-size:.6rem;color:rgba(255,255,255,.8);transition:background .5s;}

/* PROCS */
.proc-row{display:flex;align-items:center;gap:8px;margin-bottom:5px;cursor:pointer;padding:2px 0;}
.proc-row:hover .proc-name{color:var(--gold);}
.proc-name{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:.76rem;}
.proc-bar-wrap{width:90px;height:5px;background:var(--border);}
.proc-bar{height:100%;transition:width .4s;}
.proc-val{width:46px;text-align:right;font-size:.73rem;color:var(--muted);}
.proc-badge{font-size:.58rem;padding:1px 5px;margin-left:3px;}
.proc-badge.new{background:rgba(155,109,255,.15);color:var(--purple);border:1px solid rgba(155,109,255,.3);}
.proc-badge.anomaly{background:rgba(232,68,42,.12);color:var(--red);}

/* TEMPS */
.temp-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;}
.temp-label{font-size:.73rem;color:var(--muted);}
.temp-val{font-size:.76rem;}

/* PM2 */
.pm2-panel{background:var(--card);border:1px solid var(--border2);padding:14px 18px;margin-bottom:16px;}
.pm2-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}
.pm2-svc-status{font-size:.66rem;letter-spacing:.12em;font-weight:700;padding:3px 10px;}
.pm2-svc-status.online{color:var(--green);border:1px solid rgba(42,232,122,.3);background:rgba(42,232,122,.05);}
.pm2-svc-status.offline{color:var(--muted2);border:1px solid var(--border2);}
.pm2-svc-status.degraded{color:var(--red);border:1px solid rgba(232,68,42,.3);animation:pulse 1s infinite;}
.pm2-proc{display:grid;grid-template-columns:12px 1fr 64px 58px 58px 48px;gap:0 8px;
align-items:center;padding:7px 0;border-bottom:1px solid var(--border);font-size:.72rem;}
.pm2-proc:last-child{border:none;}
.pm2-dot{width:9px;height:9px;border-radius:50%;}
.pm2-dot.online{background:var(--green);box-shadow:0 0 5px var(--green);}
.pm2-dot.stopped,.pm2-dot.errored{background:var(--red);box-shadow:0 0 5px var(--red);animation:pulse .8s infinite;}
.pm2-name{color:var(--text);font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.pm2-name.critical{color:var(--gold);}

/* INCIDENT CARDS */
.incident-card{padding:8px 12px;margin-bottom:6px;border-left:3px solid var(--border2);
background:rgba(255,255,255,.02);font-size:.73rem;line-height:1.6;position:relative;}
.incident-card.critical{border-color:var(--red);background:rgba(232,68,42,.04);}
.incident-card.urgent{border-color:var(--amber);}
.incident-card.elevated{border-color:var(--blue);}
.incident-ts{font-size:.65rem;color:var(--muted2);float:right;}
.incident-pin{font-size:.63rem;background:transparent;border:1px solid var(--border2);
color:var(--muted2);padding:2px 7px;cursor:pointer;margin-left:6px;}
.incident-pin:hover{border-color:var(--gold);color:var(--gold);}

/* TIMELINE SCRUBBER */
.scrubber-wrap{position:relative;height:48px;margin-bottom:12px;background:var(--surface);border:1px solid var(--border);}
.scrubber-track{position:absolute;inset:0;cursor:crosshair;}
.scrubber-cursor{position:absolute;top:0;bottom:0;width:1px;background:var(--gold);pointer-events:none;}
.scrubber-label{position:absolute;bottom:3px;font-size:.6rem;color:var(--gold);pointer-events:none;}

/* RCA */
.rca-section{margin-bottom:14px;}
.rca-item{font-size:.73rem;padding:6px 10px;margin-bottom:4px;border-left:3px solid var(--blue);
background:rgba(58,143,212,.04);line-height:1.6;}
.rca-confidence{color:var(--teal);font-size:.68rem;}
.rca-action{color:var(--amber);font-size:.65rem;letter-spacing:.06em;padding:2px 7px;
border:1px solid rgba(232,125,42,.3);margin-left:8px;}

/* SRE */
.sre-panel{background:rgba(42,221,201,.04);border:1px solid rgba(42,221,201,.15);
padding:10px 14px;margin-bottom:14px;}
.sre-action{display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:.73rem;}
.sre-exec-btn{background:transparent;border:1px solid rgba(42,221,201,.4);color:var(--teal);
padding:3px 10px;font-family:'DM Mono',monospace;font-size:.66rem;cursor:pointer;letter-spacing:.06em;}
.sre-exec-btn:hover{background:rgba(42,221,201,.08);}

/* FORECASTS */
.forecast-bar{height:24px;background:var(--surface);border:1px solid var(--border);
position:relative;margin-bottom:5px;overflow:hidden;}
.forecast-fill{height:100%;transition:width 1s ease;position:absolute;top:0;left:0;}
.forecast-predicted{position:absolute;top:0;bottom:0;width:1px;background:rgba(201,168,76,.6);}
.forecast-label{position:absolute;right:8px;top:4px;font-size:.65rem;color:var(--muted);}

/* VERDICT HISTORY */
.hist-row{display:flex;gap:10px;margin-bottom:3px;font-size:.7rem;line-height:1.5;
border-bottom:1px solid var(--border);padding-bottom:3px;}
.hist-ts{color:var(--muted2);width:58px;flex-shrink:0;}
.hist-text{color:var(--muted);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.hist-score{color:var(--gold);width:36px;text-align:right;flex-shrink:0;}

/* TAMA */
.tama-wrap{display:flex;align-items:center;gap:12px;margin-bottom:12px;padding:10px 14px;
background:var(--surface);border:1px solid var(--border);}
.tama-face{font-size:1.6rem;min-width:34px;text-align:center;}
.tama-msg{font-size:.76rem;color:var(--muted);flex:1;line-height:1.6;}
.tama-mood{font-size:.62rem;letter-spacing:.1em;padding:2px 7px;}

/* ANNOTATIONS / SCRIBE */
.annotation{font-size:.71rem;padding:4px 10px;margin-bottom:3px;border-left:2px solid var(--purple);
background:rgba(155,109,255,.04);color:var(--muted);}
.annotation.auto{border-color:var(--muted2);}
.annotation .ann-ts{color:var(--muted2);margin-right:8px;}

/* FLASH PROCS */
.flash-proc{font-size:.66rem;color:var(--amber);padding:2px 8px;background:rgba(232,125,42,.08);
border:1px solid rgba(232,125,42,.2);margin-right:5px;display:inline-block;}

/* NETWORK */
.net-row{display:flex;justify-content:space-between;margin-bottom:3px;font-size:.71rem;}
.net-proc{color:var(--muted);flex:1;overflow:hidden;text-overflow:ellipsis;}
.net-addr{color:var(--muted2);font-size:.68rem;}
.net-status{font-size:.64rem;padding:1px 6px;margin-left:5px;}
.net-status.ESTABLISHED{color:var(--green);}
.net-status.LISTEN{color:var(--blue);}

/* AI CONFIDENCE */
.ai-conf{font-size:.66rem;color:var(--muted2);letter-spacing:.06em;}
.ai-pill{font-size:.66rem;letter-spacing:.1em;padding:3px 9px;border-radius:1px;}
.ai-pill.ready{color:var(--green);border:1px solid rgba(42,232,122,.25);}
.ai-pill.fetching{color:var(--amber);border:1px solid rgba(232,125,42,.25);animation:pulse 1s infinite;}
.ai-pill.error{color:var(--red);border:1px solid rgba(232,68,42,.25);}
.ai-pill.waiting{color:var(--muted2);border:1px solid var(--border2);}

/* MODALS */
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.88);z-index:500;
display:none;align-items:center;justify-content:center;backdrop-filter:blur(2px);}
.modal-bg.open{display:flex;}
.modal{background:var(--card);border:1px solid var(--border2);padding:28px;
max-width:620px;width:90%;position:relative;max-height:80vh;overflow-y:auto;}
.modal::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;
background:linear-gradient(90deg,var(--gold),transparent);}
.modal-title{font-family:'Syncopate',sans-serif;font-size:.8rem;letter-spacing:.2em;
color:var(--gold);margin-bottom:6px;}
.modal-sub{font-size:.7rem;color:var(--muted2);margin-bottom:16px;}
.modal-body{font-size:.78rem;color:var(--text);line-height:1.8;white-space:pre-wrap;}
.modal-close{margin-top:18px;background:transparent;border:1px solid var(--border2);
color:var(--muted);padding:5px 16px;font-family:'DM Mono',monospace;font-size:.72rem;cursor:pointer;
letter-spacing:.1em;}
.modal-close:hover{border-color:var(--gold);color:var(--gold);}

/* TERMINAL */
.terminal-panel{background:#000;border:1px solid var(--border2);padding:12px;
font-family:'DM Mono',monospace;font-size:.76rem;height:240px;overflow-y:auto;margin-bottom:14px;}
.terminal-panel .t-line{color:#33ff33;margin-bottom:2px;}
.terminal-panel .t-err{color:var(--red);}
.terminal-input-row{display:flex;gap:8px;margin-bottom:10px;}
#terminal-input{flex:1;background:transparent;border:1px solid var(--border2);color:var(--text);
padding:6px 10px;font-family:'DM Mono',monospace;font-size:.76rem;outline:none;}
.terminal-send{background:transparent;border:1px solid var(--border2);color:var(--muted);
padding:5px 12px;font-family:'DM Mono',monospace;font-size:.72rem;cursor:pointer;}

/* HEATMAP CAL */
.heatmap-grid{display:grid;grid-template-columns:repeat(24,1fr);gap:3px;margin-bottom:10px;}
.heatmap-cell{height:18px;border-radius:2px;cursor:pointer;transition:opacity .2s;}
.heatmap-cell:hover{opacity:.7;}

/* CMD PALETTE */
#cmdpalette-bg{position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:600;
display:none;align-items:flex-start;justify-content:center;padding-top:80px;}
#cmdpalette-bg.open{display:flex;}
#cmdpalette{background:var(--card);border:1px solid var(--border2);width:560px;
padding:0;overflow:hidden;}
#palette-input{width:100%;background:transparent;border:none;border-bottom:1px solid var(--border2);
color:var(--text);padding:14px 18px;font-family:'DM Mono',monospace;font-size:.85rem;outline:none;}
.palette-item{padding:10px 18px;font-size:.76rem;cursor:pointer;display:flex;
justify-content:space-between;align-items:center;}
.palette-item:hover,.palette-item.selected{background:var(--surface);}
.palette-item .pi-label{color:var(--text);}
.palette-item .pi-kbd{color:var(--muted2);font-size:.65rem;}

/* TABS */
.tab-bar{display:flex;border-bottom:1px solid var(--border);background:var(--surface);padding:0 24px;position:sticky;top:0;z-index:99;}
.tab-btn{background:transparent;border:none;color:var(--muted2);padding:10px 18px;font-family:'DM Mono',monospace;
font-size:.68rem;letter-spacing:.1em;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;}
.tab-btn:hover{color:var(--muted);}
.tab-btn.active{color:var(--gold);border-bottom-color:var(--gold);}
.tab-panel{display:none;}
.tab-panel.active{display:block;}

/* NETWORK PANEL */
.net-panel{padding:16px 24px;}
.net-table{width:100%;border-collapse:collapse;font-size:.72rem;}
.net-table th{color:var(--muted2);font-size:.62rem;letter-spacing:.1em;padding:4px 8px;
text-align:left;border-bottom:1px solid var(--border);font-weight:400;}
.net-table td{padding:5px 8px;border-bottom:1px solid var(--border);color:var(--muted);}
.net-table td:first-child{color:var(--text);}
.net-flag{font-size:.6rem;padding:1px 6px;margin-left:4px;}
.net-flag.new{color:var(--amber);border:1px solid rgba(232,125,42,.3);}
.net-flag.suspicious{color:var(--red);border:1px solid rgba(232,68,42,.3);}

/* INTEL PANEL */
.intel-panel{display:grid;grid-template-columns:1fr 1fr;gap:0;}
.intel-col{padding:16px 24px;border-right:1px solid var(--border);}
.intel-col:last-child{border-right:none;}
.intel-item{padding:8px 12px;margin-bottom:6px;border-left:3px solid var(--border2);
background:rgba(255,255,255,.02);font-size:.73rem;line-height:1.6;}
.intel-item.high{border-color:var(--red);}
.intel-item.med{border-color:var(--amber);}
.intel-item.low{border-color:var(--blue);}
.intel-label{font-size:.6rem;letter-spacing:.1em;color:var(--muted2);margin-bottom:2px;}

/* SCRIBE PANEL */
.scribe-entry{display:flex;gap:10px;padding:6px 0;border-bottom:1px solid var(--border);font-size:.72rem;}
.scribe-ts{color:var(--muted2);width:60px;flex-shrink:0;font-size:.65rem;}
.scribe-type{width:80px;flex-shrink:0;}
.scribe-msg{color:var(--muted);flex:1;}

/* LENS FOCUS */
.lens-card{padding:16px 20px;background:var(--surface);border:1px solid var(--border2);
margin-bottom:12px;position:relative;}
.lens-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
background:linear-gradient(90deg,var(--gold),transparent);}
.lens-priority{font-size:2rem;font-family:'Syncopate',sans-serif;color:var(--gold);font-weight:700;}
.lens-label{font-size:.65rem;letter-spacing:.12em;color:var(--muted2);margin-bottom:4px;}
.lens-desc{font-size:.76rem;color:var(--text);line-height:1.6;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
@keyframes goldpulse{0%,100%{opacity:1}50%{opacity:.5}}
footer{border-top:1px solid var(--border);padding:10px 24px;display:flex;
justify-content:space-between;font-size:.66rem;color:var(--muted2);letter-spacing:.08em;}
.freeze-overlay{position:fixed;inset:0;z-index:900;border:2px solid var(--amber);
pointer-events:none;display:none;}
.freeze-overlay.active{display:block;}
.freeze-label{position:fixed;top:60px;left:50%;transform:translateX(-50%);
background:rgba(232,125,42,.15);border:1px solid var(--amber);color:var(--amber);
padding:3px 14px;font-size:.58rem;letter-spacing:.15em;z-index:901;display:none;}
.freeze-label.active{display:block;}
</style>
</head>
<body>
<div class="scan-line"></div>
<div class="freeze-overlay" id="freeze-overlay"></div>
<div class="freeze-label" id="freeze-label">⬡ FROZEN — PRESS F TO RESUME</div>

<!-- HEADER -->
<header>
  <div class="brand">SYSWATCH <span>V5</span></div>
  <div class="header-center">
    <div id="clock" style="font-size:.6rem;letter-spacing:.1em;color:var(--muted)"></div>
    <div class="kbd-hint">F=freeze · 1/2=focus · Q=query · K=palette · Esc=close</div>
  </div>
  <div class="header-right">
    <div id="local-ai-pill" class="ai-pill waiting"><span>LOCAL AI</span></div>
    <div id="oracle-ai-pill" class="ai-pill waiting"><span>ORACLE AI</span></div>
    <button id="freeze-btn" onclick="toggleFreeze()">FREEZE</button>
  </div>
</header>

<!-- PRIORITY FEED -->
<div id="priority-feed">
  <div class="feed-label">PRIORITY ·</div>
  <div id="feed-items" style="display:flex;gap:6px;"></div>
</div>

<!-- COMMAND BAR -->
<div id="cmdbar">
  <input id="cmd-input" placeholder="Ask AI about your system... (Enter to send)" autocomplete="off"/>
  <select id="cmd-machine" class="cmd-machine-sel">
    <option value="local">LOCAL</option>
    <option value="oracle">ORACLE</option>
  </select>
  <button class="cmd-btn" onclick="sendCmd()">ASK</button>
  <button class="cmd-btn" onclick="triggerPostmortem(document.getElementById('cmd-machine').value)">POST-MORTEM</button>
  <button class="cmd-btn" onclick="exportSession()">EXPORT</button>
</div>
<div id="cmd-resp"></div>

<!-- NARRATIVE BAR -->
<div id="narrative-bar">Initializing cross-machine intelligence…</div>

<!-- THETA CONTEXT BAR -->
<div id="theta-bar" style="background:var(--surface);border-bottom:1px solid var(--border);
padding:4px 20px;font-size:.5rem;color:var(--muted2);letter-spacing:.1em;">
  Initializing time context…
</div>

<!-- TAB BAR — sits right below narrative/theta, above all content -->
<div class="tab-bar">
  <button class="tab-btn active" onclick="switchTab('overview')" id="tab-overview">OVERVIEW</button>
  <button class="tab-btn" onclick="switchTab('intelligence')" id="tab-intelligence">INTELLIGENCE</button>
  <button class="tab-btn" onclick="switchTab('network')" id="tab-network">NETWORK</button>
  <button class="tab-btn" onclick="switchTab('palantir-tab')" id="tab-palantir-tab">PALANTIR</button>
</div>

<!-- OVERVIEW TAB -->
<div class="tab-panel active" id="panel-overview">

<!-- DIFF OVERLAY -->
<div style="background:var(--surface);border-bottom:1px solid var(--border);padding:10px 20px;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
    <div style="font-size:.52rem;letter-spacing:.15em;color:var(--muted2)">DIFF OVERLAY — LOCAL vs ORACLE CPU</div>
    <div style="font-size:.5rem;color:var(--muted2)">GREEN=LOCAL · GOLD=ORACLE</div>
  </div>
  <div id="diff-overlay-wrap" style="height:70px;position:relative;background:var(--card);border:1px solid var(--border)"></div>
</div>

<!-- TIMELINE SCRUBBER -->
<div style="background:var(--surface);border-bottom:1px solid var(--border);padding:10px 20px;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
    <div style="font-size:.52rem;letter-spacing:.15em;color:var(--muted2)">TIMELINE — DRAG TO SELECT WINDOW → AI QUERY</div>
    <div style="font-size:.5rem;color:var(--muted2)">1h · GREEN=LOCAL CPU · GOLD=ORACLE CPU</div>
  </div>
  <div id="scrubber-wrap" style="height:60px;position:relative;background:var(--card);border:1px solid var(--border)"></div>
</div>

<!-- SESSION REPLAY -->
<div style="background:var(--surface);border-bottom:1px solid var(--border);padding:8px 20px;display:flex;gap:12px;align-items:center;">
  <div style="font-size:.52rem;letter-spacing:.15em;color:var(--muted2);white-space:nowrap">SESSION REPLAY</div>
  <button id="replay-play-btn" class="cmd-btn" style="font-size:.52rem;padding:3px 10px" onclick="replayPlay()">▶ PLAY</button>
  <input id="replay-slider" type="range" min="0" max="100" value="0"
    style="flex:1;accent-color:var(--gold);height:3px"
    oninput="replayScrub(this.value)"/>
  <div id="replay-label" style="font-size:.52rem;color:var(--gold);white-space:nowrap;width:60px;text-align:right">—</div>
  <div style="display:flex;gap:6px;align-items:center;min-width:160px">
    <div style="font-size:.5rem;color:var(--muted2)">LOCAL CPU</div>
    <div style="flex:1;height:4px;background:var(--border);position:relative">
      <div id="replay-local-bar" style="height:100%;width:0;background:var(--green);transition:width .3s"></div>
    </div>
    <div style="font-size:.5rem;color:var(--muted2)">ORACLE</div>
    <div style="flex:1;height:4px;background:var(--border);position:relative">
      <div id="replay-oracle-bar" style="height:100%;width:0;background:var(--gold);transition:width .3s"></div>
    </div>
  </div>
  <button class="cmd-btn" style="font-size:.52rem" onclick="loadReplay()">RELOAD</button>
</div>

<!-- HEATMAP CALENDAR -->
<div style="background:var(--surface);border-bottom:1px solid var(--border);padding:8px 20px;">
  <div style="font-size:.52rem;letter-spacing:.15em;color:var(--muted2);margin-bottom:6px">
    HEATMAP — LOCAL CPU BY HOUR (LAST 24H) · CLICK HOUR TO ANALYZE
  </div>
  <div id="heatmap-cal"></div>
</div>

<!-- FLUX DIFF FEED -->
<div style="background:var(--surface);border-bottom:1px solid var(--border);padding:8px 20px;">
  <div style="font-size:.52rem;letter-spacing:.15em;color:var(--muted2);margin-bottom:6px">STATE CHANGES</div>
  <div id="flux-feed"></div>
</div>

<div class="main-grid">
  <!-- LOCAL MAC -->
  <div class="machine-col" id="local-col">
    <div class="status-row">
      <div class="machine-label" style="color:var(--teal)">LOCAL MAC</div>
      <div style="display:flex;gap:6px;align-items:center;">
        <div class="status-badge online" id="local-status">ONLINE</div>
        <div id="local-ai-pill2" class="ai-pill waiting"><span>AI</span></div>
      </div>
    </div>
    <div class="meta-row" id="local-meta">—</div>

    <!-- Score -->
    <div class="score-section">
      <div>
        <div class="score-label">HEALTH</div>
        <div><span class="score-num" id="local-score-num" style="color:var(--green)">1000</span><span class="score-grade" id="local-score-grade">S</span></div>
      </div>
      <div class="score-spark-wrap"><canvas id="local-score-spark"></canvas></div>
      <button class="cmd-btn" style="font-size:.5rem" onclick="whyScore('local')">WHY?</button>
    </div>

    <!-- CPU -->
    <div class="gauge-section">
      <div class="gauge-row">
        <span class="gauge-label">CPU</span>
        <span><span class="gauge-pct" id="local-cpu-pct" style="color:var(--text)">0</span><span style="font-size:.6rem">%</span>
        <span class="gauge-tier" id="local-cpu-tier"></span></span>
      </div>
      <div class="gauge-bar-wrap"><div class="gauge-bar" id="local-cpu-bar"></div></div>
      <canvas class="spark" id="local-cpu-spark"></canvas>
      <div class="gauge-verdict" id="local-cpu-verdict">Initializing…</div>
      <div class="gauge-forecast" id="local-cpu-forecast"></div>
    </div>

    <!-- RAM -->
    <div class="gauge-section">
      <div class="gauge-row">
        <span class="gauge-label">RAM</span>
        <span><span class="gauge-pct" id="local-ram-pct" style="color:var(--text)">0</span><span style="font-size:.6rem">%</span>
        <span class="gauge-tier" id="local-ram-tier"></span></span>
      </div>
      <div class="gauge-bar-wrap"><div class="gauge-bar" id="local-ram-bar"></div></div>
      <canvas class="spark" id="local-ram-spark"></canvas>
      <div class="gauge-verdict" id="local-ram-verdict">Initializing…</div>
      <div class="gauge-forecast" id="local-ram-forecast"></div>
    </div>

    <div class="divider"></div>

    <!-- CPU Cores heatmap -->
    <div class="section-title">CPU CORES</div>
    <div class="core-grid" id="local-cores"></div>

    <!-- Processes -->
    <div class="section-title">TOP CPU</div>
    <div id="local-cpu-procs"></div>
    <div class="section-title" style="margin-top:8px">TOP RAM</div>
    <div id="local-ram-procs"></div>

    <div class="divider"></div>

    <!-- Root cause -->
    <div class="section-title">ROOT CAUSE ANALYSIS</div>
    <div id="local-rca" class="rca-section"></div>

    <!-- SRE proposals -->
    <div id="local-sre" class="sre-panel" style="display:none">
      <div class="section-title" style="margin-bottom:6px;border:none">⚡ REPAIR PROPOSALS</div>
      <div id="local-sre-actions"></div>
    </div>

    <!-- Forecasts -->
    <div class="section-title">FORECASTS (5 MIN)</div>
    <div id="local-forecasts" style="margin-bottom:10px"></div>

    <!-- Flash procs -->
    <div id="local-flash-row" style="margin-bottom:8px;display:none">
      <div class="section-title">MICRO — FLASH PROCESSES</div>
      <div id="local-flash-procs"></div>
    </div>

    <!-- Incidents -->
    <div class="section-title">INCIDENTS</div>
    <div id="local-incidents" style="margin-bottom:10px"></div>

    <!-- Temps -->
    <div id="local-temps-wrap" style="margin-bottom:10px"></div>

    <!-- Network -->
    <div class="section-title">NETWORK</div>
    <div id="local-net" style="margin-bottom:8px"></div>

    <!-- Runbook -->
    <div id="local-runbook" style="display:none;margin-bottom:10px;padding:6px 10px;
    background:rgba(155,109,255,.05);border-left:2px solid var(--purple)">
      <div style="font-size:.5rem;letter-spacing:.12em;color:var(--purple);margin-bottom:4px">RUNBOOK — RECURRING PATTERN DETECTED</div>
      <div id="local-runbook-text" style="font-size:.58rem;color:var(--muted);line-height:1.6"></div>
    </div>

    <!-- Annotations -->
    <div class="section-title">ANNOTATIONS</div>
    <div id="local-annotations" style="margin-bottom:10px"></div>

    <!-- Verdict history -->
    <div class="section-title">VERDICT HISTORY</div>
    <div id="local-hist"></div>

    <!-- TAMA -->
    <div id="local-tama" class="tama-wrap" style="margin-top:10px">
      <div class="tama-face" id="local-tama-face">(－‿－)</div>
      <div class="tama-msg" id="local-tama-msg">Monitoring…</div>
    </div>

    <div style="display:flex;gap:6px;margin-top:8px;">
      <button class="cmd-btn" onclick="triggerPostmortem('local')">POST-MORTEM</button>
      <button class="cmd-btn" onclick="openSpotlightPicker('local')">SPOTLIGHT</button>
    </div>
  </div>

  <!-- ORACLE CLOUD -->
  <div class="machine-col" id="oracle-col">
    <div class="status-row">
      <div class="machine-label" style="color:var(--gold)">ORACLE CLOUD</div>
      <div style="display:flex;gap:6px;align-items:center;">
        <div class="status-badge offline" id="oracle-status">OFFLINE</div>
        <div id="oracle-ai-pill2" class="ai-pill waiting"><span>AI</span></div>
      </div>
    </div>
    <div class="meta-row" id="oracle-meta">—</div>

    <!-- Score -->
    <div class="score-section">
      <div>
        <div class="score-label">HEALTH</div>
        <div><span class="score-num" id="oracle-score-num" style="color:var(--green)">—</span><span class="score-grade" id="oracle-score-grade"></span></div>
      </div>
      <div class="score-spark-wrap"><canvas id="oracle-score-spark"></canvas></div>
      <button class="cmd-btn" style="font-size:.5rem" onclick="whyScore('oracle')">WHY?</button>
    </div>

    <!-- CPU -->
    <div class="gauge-section">
      <div class="gauge-row">
        <span class="gauge-label">CPU</span>
        <span><span class="gauge-pct" id="oracle-cpu-pct" style="color:var(--text)">—</span><span style="font-size:.6rem" id="oracle-cpu-pct-sym">%</span>
        <span class="gauge-tier" id="oracle-cpu-tier"></span></span>
      </div>
      <div class="gauge-bar-wrap"><div class="gauge-bar" id="oracle-cpu-bar"></div></div>
      <canvas class="spark" id="oracle-cpu-spark"></canvas>
      <div class="gauge-verdict" id="oracle-cpu-verdict">Waiting for Oracle…</div>
      <div class="gauge-forecast" id="oracle-cpu-forecast"></div>
    </div>

    <!-- RAM -->
    <div class="gauge-section">
      <div class="gauge-row">
        <span class="gauge-label">RAM</span>
        <span><span class="gauge-pct" id="oracle-ram-pct" style="color:var(--text)">—</span><span style="font-size:.6rem" id="oracle-ram-pct-sym">%</span>
        <span class="gauge-tier" id="oracle-ram-tier"></span></span>
      </div>
      <div class="gauge-bar-wrap"><div class="gauge-bar" id="oracle-ram-bar"></div></div>
      <canvas class="spark" id="oracle-ram-spark"></canvas>
      <div class="gauge-verdict" id="oracle-ram-verdict">Waiting for Oracle…</div>
      <div class="gauge-forecast" id="oracle-ram-forecast"></div>
    </div>

    <div class="divider"></div>

    <!-- PM2 -->
    <div class="pm2-panel">
      <div class="pm2-header">
        <div class="section-title" style="margin-bottom:0;border:none">SERVICES · PM2</div>
        <div class="pm2-svc-status offline" id="pm2-svc-status">CONNECTING</div>
      </div>
      <div id="pm2-alerts"></div>
      <div id="pm2-proc-list"></div>
    </div>

    <!-- CPU Cores -->
    <div class="section-title">CPU CORES (ARM)</div>
    <div class="core-grid" id="oracle-cores"></div>

    <!-- Processes -->
    <div class="section-title">TOP CPU</div>
    <div id="oracle-cpu-procs"></div>
    <div class="section-title" style="margin-top:8px">TOP RAM</div>
    <div id="oracle-ram-procs"></div>

    <div class="divider"></div>

    <!-- Root cause -->
    <div class="section-title">ROOT CAUSE ANALYSIS</div>
    <div id="oracle-rca" class="rca-section"></div>

    <!-- SRE -->
    <div id="oracle-sre" class="sre-panel" style="display:none">
      <div class="section-title" style="margin-bottom:6px;border:none">⚡ REPAIR PROPOSALS</div>
      <div id="oracle-sre-actions"></div>
    </div>

    <!-- Forecasts -->
    <div class="section-title">FORECASTS (5 MIN)</div>
    <div id="oracle-forecasts" style="margin-bottom:10px"></div>

    <!-- Flash procs -->
    <div id="oracle-flash-row" style="margin-bottom:8px;display:none">
      <div class="section-title">MICRO — FLASH PROCESSES</div>
      <div id="oracle-flash-procs"></div>
    </div>

    <!-- Incidents -->
    <div class="section-title">INCIDENTS</div>
    <div id="oracle-incidents" style="margin-bottom:10px"></div>

    <!-- Temps -->
    <div id="oracle-temps-wrap" style="margin-bottom:10px"></div>

    <!-- Network -->
    <div class="section-title">NETWORK</div>
    <div id="oracle-net" style="margin-bottom:8px"></div>

    <!-- Runbook -->
    <div id="oracle-runbook" style="display:none;margin-bottom:10px;padding:6px 10px;
    background:rgba(155,109,255,.05);border-left:2px solid var(--purple)">
      <div style="font-size:.5rem;letter-spacing:.12em;color:var(--purple);margin-bottom:4px">RUNBOOK — RECURRING PATTERN DETECTED</div>
      <div id="oracle-runbook-text" style="font-size:.58rem;color:var(--muted);line-height:1.6"></div>
    </div>

    <!-- Annotations -->
    <div class="section-title">ANNOTATIONS</div>
    <div id="oracle-annotations" style="margin-bottom:10px"></div>

    <!-- Verdict history -->
    <div class="section-title">VERDICT HISTORY</div>
    <div id="oracle-hist"></div>

    <!-- TAMA -->
    <div id="oracle-tama" class="tama-wrap" style="margin-top:10px">
      <div class="tama-face" id="oracle-tama-face">(－‿－)</div>
      <div class="tama-msg" id="oracle-tama-msg">Waiting for Oracle…</div>
    </div>

    <div style="display:flex;gap:6px;margin-top:8px;">
      <button class="cmd-btn" onclick="triggerPostmortem('oracle')">POST-MORTEM</button>
      <button class="cmd-btn" onclick="openSpotlightPicker('oracle')">SPOTLIGHT</button>
      <button class="cmd-btn" onclick="openTerminal('oracle')">TERMINAL</button>
    </div>
  </div>
</div>
</div><!-- end panel-overview -->

<!-- INTELLIGENCE TAB -->
<div class="tab-panel" id="panel-intelligence">
  <div class="intel-panel">
    <!-- LEFT: LENS + SCRIBE -->
    <div class="intel-col">
      <div class="section-title">LENS · FOCUS NOW</div>
      <div id="lens-focus-card" class="lens-card" style="margin-bottom:16px;">
        <div class="lens-label">MOST IMPORTANT RIGHT NOW</div>
        <div class="lens-priority" id="lens-score">—</div>
        <div class="lens-desc" id="lens-desc">Analyzing system state…</div>
      </div>

      <div class="section-title">VIGIL · ACTIVE THREATS</div>
      <div id="vigil-list" style="margin-bottom:16px;"></div>

      <div class="section-title">MOSAIC · PATTERNS</div>
      <div id="mosaic-list" style="margin-bottom:16px;"></div>

      <div class="section-title">CODEX · RECENT EVENTS</div>
      <div id="codex-list"></div>
    </div>

    <!-- RIGHT: SCRIBE + VECTORS -->
    <div class="intel-col">
      <div class="section-title">SCRIBE · ANNOTATION LOG</div>
      <div id="scribe-list" style="margin-bottom:16px;max-height:280px;overflow-y:auto;"></div>

      <div class="section-title">VECTOR · METRIC DIRECTION</div>
      <div id="vector-list" style="margin-bottom:16px;"></div>

      <div class="section-title">ATLAS · FORECASTS</div>
      <div id="atlas-list" style="margin-bottom:16px;"></div>

      <div class="section-title">PDCM · LEARNED PREFERENCES</div>
      <div id="pdcm-profile" style="font-size:.72rem;color:var(--muted);"></div>
    </div>
  </div>
</div>

<!-- NETWORK TAB -->
<div class="tab-panel" id="panel-network">
  <div class="net-panel">
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;">
      <div>
        <div class="section-title">LOCAL MAC · CONNECTIONS</div>
        <table class="net-table" id="net-local-table">
          <thead><tr>
            <th>PROCESS</th><th>REMOTE</th><th>STATUS</th>
          </tr></thead>
          <tbody id="net-local-body">
            <td colspan="3" style="color:var(--muted2);padding:8px;">Loading…</td>
          </tbody>
        </table>
      </div>
      <div>
        <div class="section-title">ORACLE · CONNECTIONS</div>
        <table class="net-table" id="net-oracle-table">
          <thead><tr>
            <th>PROCESS</th><th>REMOTE</th><th>STATUS</th>
          </tr></thead>
          <tbody id="net-oracle-body">
            <td colspan="3" style="color:var(--muted2);padding:8px;">Loading…</td>
          </tbody>
        </table>
      </div>
    </div>

    <div style="margin-top:20px;">
      <div class="section-title">SENTINEL · NEW OUTBOUND CONNECTIONS</div>
      <div id="sentinel-new-conns" style="font-size:.72rem;color:var(--muted);"></div>
    </div>

    <div style="margin-top:16px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;">
      <div>
        <div class="section-title">PUBLIC IP</div>
        <div id="net-pub-ip" style="font-size:1.1rem;color:var(--teal);font-family:'Syncopate',sans-serif;">—</div>
      </div>
      <div>
        <div class="section-title">DEVICES ON NETWORK</div>
        <div id="net-device-count" style="font-size:1.1rem;color:var(--green);font-family:'Syncopate',sans-serif;">—</div>
      </div>
      <div>
        <div class="section-title">BT ANCHORS VISIBLE</div>
        <div id="net-bt-anchors" style="font-size:1.1rem;color:var(--gold);font-family:'Syncopate',sans-serif;">—</div>
      </div>
    </div>
  </div>
</div>

<!-- PALANTIR TAB -->
<div class="tab-panel" id="panel-palantir-tab">
  <div id="palantir-tab-content" style="padding:0;">
    <div style="padding:16px 24px;color:var(--muted2);font-size:.72rem;">Loading Palantir…</div>
  </div>
</div>

<footer>
  <span>SYSWATCH V5 · MULTI-SYSTEM INTELLIGENCE</span>
  <span id="footer-ts">—</span>
  <span>DATA 6s · AI ADAPTIVE · 40-SYSTEM BACKEND</span>
</footer>

<!-- MODALS -->
<div class="modal-bg" id="proc-modal">
  <div class="modal">
    <div class="modal-title" id="proc-modal-title">PROCESS SPOTLIGHT</div>
    <div class="modal-sub" id="proc-modal-sub"></div>
    <div class="modal-body" id="proc-modal-body">Consulting AI…</div>
    <button class="modal-close" onclick="closeModal('proc-modal')">CLOSE</button>
  </div>
</div>
<div class="modal-bg" id="post-modal">
  <div class="modal">
    <div class="modal-title">POST-MORTEM ANALYSIS</div>
    <div class="modal-sub" id="post-modal-sub"></div>
    <div class="modal-body" id="post-modal-body">Analyzing…</div>
    <button class="modal-close" onclick="closeModal('post-modal')">CLOSE</button>
  </div>
</div>
<div class="modal-bg" id="why-modal">
  <div class="modal">
    <div class="modal-title">HEALTH SCORE ANALYSIS</div>
    <div class="modal-sub" id="why-modal-sub"></div>
    <div class="modal-body" id="why-modal-body">Analyzing session…</div>
    <button class="modal-close" onclick="closeModal('why-modal')">CLOSE</button>
  </div>
</div>
<div class="modal-bg" id="incident-modal">
  <div class="modal">
    <div class="modal-title">INCIDENT ANALYSIS</div>
    <div class="modal-sub" id="incident-modal-sub"></div>
    <div class="modal-body" id="incident-modal-body">Reconstructing causal chain…</div>
    <button class="modal-close" onclick="closeModal('incident-modal')">CLOSE</button>
  </div>
</div>
<div class="modal-bg" id="pin-modal">
  <div class="modal">
    <div class="modal-title">PIN MOMENT</div>
    <div class="modal-sub">Add a note to this moment</div>
    <input id="pin-note" style="width:100%;background:transparent;border:1px solid var(--border2);
    color:var(--text);padding:6px;font-family:'DM Mono',monospace;font-size:.65rem;margin-bottom:10px;outline:none;"
    placeholder="What happened here?"/>
    <input type="hidden" id="pin-machine"/>
    <button class="modal-close" onclick="submitPin()">PIN IT</button>
    <button class="modal-close" style="margin-left:6px" onclick="closeModal('pin-modal')">CANCEL</button>
  </div>
</div>
<div class="modal-bg" id="terminal-modal">
  <div class="modal" style="max-width:640px">
    <div class="modal-title">ORACLE TERMINAL</div>
    <div class="modal-sub">Direct shell access — commands execute on Oracle Cloud</div>
    <div class="terminal-panel" id="terminal-output"></div>
    <div class="terminal-input-row">
      <input id="terminal-input" placeholder="$ enter command..." autocomplete="off"/>
      <button class="terminal-send" onclick="sendTerminalCmd()">RUN</button>
    </div>
    <button class="modal-close" onclick="closeModal('terminal-modal')">CLOSE</button>
  </div>
</div>

<!-- CMD PALETTE -->
<div id="cmdpalette-bg" onclick="if(event.target===this)closePalette()">
  <div id="cmdpalette">
    <input id="palette-input" placeholder="Type a command or question…" autocomplete="off"/>
    <div id="palette-list"></div>
  </div>
</div>

<script>
// ── STATE ────────────────────────────────────────────────────────────────────
// Anti-flicker helpers — only touch the DOM when content actually changes
function setIfChanged(id, html) {
  const el = document.getElementById(id);
  if (el && el.innerHTML !== html) el.innerHTML = html;
}
function setTextIfChanged(id, text) {
  const el = document.getElementById(id);
  if (el && el.textContent !== text) el.textContent = text;
}
function setStyleIfChanged(el, prop, val) {
  if (el && el.style[prop] !== val) el.style[prop] = val;
}
// Debounce map for slow-refresh sections (anomaly cards, causality, quiet hours)
const _slowRefreshTs = {};
function shouldRefreshSlow(key, intervalMs = 5000) {
  const now = Date.now();
  if (!_slowRefreshTs[key] || now - _slowRefreshTs[key] > intervalMs) {
    _slowRefreshTs[key] = now;
    return true;
  }
  return false;
}
let frozen = false;
let lastData = {local: null, oracle: null};
let focusMachine = null;
let scoreHistLocal = [], scoreHistOracle = [];

const TAMA_FACES = {
  happy:    ['(＾▽＾)','(◕‿◕)','(ﾉ◕ヮ◕)ﾉ'],
  content:  ['(－‿－)','( ˘ω˘ )','(￣ω￣)'],
  concerned:['(ó_ò)','(⊙_⊙)','(°ロ°)'],
  sweating: ['(;´Д`)','(╥_╥)','(ﾟДﾟ;)'],
  screaming:['(；ﾟ〇ﾟ)','(☉_☉)','ヾ(°□°)ﾉ']
};
const TAMA_COLORS = {
  happy:'#2ae87a',content:'#55dd88',concerned:'#e87d2a',sweating:'#e8442a',screaming:'#ff1a3c'
};

// ── TIER ─────────────────────────────────────────────────────────────────────
const T=[
  [40,'CHILL','#2ae87a','chill'],
  [60,'NORMAL','#55dd88','normal'],
  [75,'ELEVATED','#e87d2a','elevated'],
  [90,'HOT','#e8442a','hot'],
  [101,'CRITICAL','#ff1a3c','critical'],
];
function tier(p){for(const[t,l,c,cl]of T)if(p<t)return{label:l,color:c,cls:cl};return{label:T[4][1],color:T[4][2],cls:T[4][3]};}
function scoreColor(s){return s>=800?'#2ae87a':s>=600?'#e8c97a':s>=400?'#e87d2a':s>=200?'#e8442a':'#ff1a3c';}
function coreColor(p){
  if(p<20)return'#1a2a1a';
  if(p<40)return'#1a3a2a';
  if(p<60)return'#2ae87a33';
  if(p<75)return'#e87d2a55';
  if(p<90)return'#e8442a77';
  return'#ff1a3c';
}

// ── TYPEWRITER ────────────────────────────────────────────────────────────────
function typewrite(el,text,speed=16){
  if(el._twTimer){clearTimeout(el._twTimer);el._twTimer=null;}
  el.textContent='';
  let i=0;
  const tick=()=>{
    if(i<text.length){el.textContent+=text[i++];el._twTimer=setTimeout(tick,speed);}
    else{el._twTimer=null;}
  };
  tick();
}

// ── SPARKLINES ────────────────────────────────────────────────────────────────
function drawSpark(id,data,color,threshold){
  const el=document.getElementById(id);
  if(!el||!data||!data.length)return;
  const W=el.offsetWidth||300,H=36,dpr=window.devicePixelRatio||1;
  el.width=W*dpr;el.height=H*dpr;
  const ctx=el.getContext('2d');
  ctx.scale(dpr,dpr);
  ctx.clearRect(0,0,W,H);
  const max=Math.max(...data,100);
  const pts=data.map((v,i)=>({x:i/(data.length-1)*W,y:H-(v/max)*H}));
  // fill
  ctx.beginPath();
  ctx.moveTo(pts[0].x,H);
  pts.forEach(p=>ctx.lineTo(p.x,p.y));
  ctx.lineTo(pts[pts.length-1].x,H);
  ctx.closePath();
  const grad=ctx.createLinearGradient(0,0,0,H);
  grad.addColorStop(0,color+'44');
  grad.addColorStop(1,'transparent');
  ctx.fillStyle=grad;ctx.fill();
  // line
  ctx.beginPath();
  pts.forEach((p,i)=>i?ctx.lineTo(p.x,p.y):ctx.moveTo(p.x,p.y));
  ctx.strokeStyle=color;ctx.lineWidth=1.2;ctx.stroke();
  // threshold line
  if(threshold){
    const ty=H-(threshold/max)*H;
    ctx.beginPath();ctx.moveTo(0,ty);ctx.lineTo(W,ty);
    ctx.strokeStyle='rgba(232,68,42,.3)';ctx.lineWidth=1;
    ctx.setLineDash([3,3]);ctx.stroke();ctx.setLineDash([]);
  }
}

function drawScoreSpark(id,data){
  const el=document.getElementById(id);
  if(!el||!data||!data.length)return;
  const W=el.offsetWidth||100,H=28,dpr=window.devicePixelRatio||1;
  el.width=W*dpr;el.height=H*dpr;
  const ctx=el.getContext('2d');ctx.scale(dpr,dpr);
  ctx.clearRect(0,0,W,H);
  const pts=data.map((v,i)=>({x:i/(Math.max(data.length-1,1))*W,y:H-(v/1000)*H}));
  ctx.beginPath();
  pts.forEach((p,i)=>i?ctx.lineTo(p.x,p.y):ctx.moveTo(p.x,p.y));
  ctx.strokeStyle=scoreColor(data[data.length-1]);
  ctx.lineWidth=1.5;ctx.stroke();
}

// ── SPARKLINE DRAG-SELECT → AI QUERY ─────────────────────────────────────────
// Attach drag-select to a spark canvas after it's drawn.
// Fires /api/incident query for the selected time window.
const _sparkDrag = {}; // id -> {active, x0, x1}

function attachSparkDrag(canvasId, machine, metric, historyLen) {
  const el = document.getElementById(canvasId);
  if(!el || el.dataset.dragAttached) return;
  el.dataset.dragAttached = '1';
  el.style.cursor = 'crosshair';
  const state = {active: false, x0: 0, x1: 0};
  _sparkDrag[canvasId] = state;

  el.addEventListener('mousedown', e => {
    state.active = true; state.x0 = e.offsetX; state.x1 = e.offsetX;
  });
  el.addEventListener('mousemove', e => {
    if(!state.active) return;
    state.x1 = e.offsetX;
    // Draw selection overlay
    const dpr = window.devicePixelRatio||1;
    const W   = el.offsetWidth, H = el.offsetHeight;
    const ctx = el.getContext('2d');
    // Don't clear — just draw translucent box on top
    const lx = Math.min(state.x0, e.offsetX), rx = Math.max(state.x0, e.offsetX);
    ctx.fillStyle = 'rgba(201,168,76,.18)';
    ctx.fillRect(lx, 0, rx-lx, H);
    ctx.strokeStyle = 'rgba(201,168,76,.5)';
    ctx.lineWidth = 1;
    ctx.strokeRect(lx, 0, rx-lx, H);
  });
  el.addEventListener('mouseup', e => {
    if(!state.active) return;
    state.active = false;
    const lx = Math.min(state.x0, e.offsetX), rx = Math.max(state.x0, e.offsetX);
    if(rx - lx < 6) return; // too small, ignore
    // Map pixel range to approximate timestamp
    const W = el.offsetWidth;
    const now = Date.now()/1000;
    const windowS = historyLen; // history length in seconds
    const tsStart = now - windowS + (lx/W)*windowS;
    const tsEnd   = now - windowS + (rx/W)*windowS;
    const midTs   = (tsStart + tsEnd)/2;
    const respEl  = document.getElementById('cmd-resp');
    respEl.style.display = 'block';
    typewrite(respEl, `Analyzing ${metric.toUpperCase()} window ${new Date(tsStart*1000).toLocaleTimeString()}–${new Date(tsEnd*1000).toLocaleTimeString()}…`, 12);
    fetch(`/api/incident?machine=${machine}&ts=${midTs}`)
      .then(r=>r.json())
      .then(d=>typewrite(respEl, `[${machine.toUpperCase()} ${metric.toUpperCase()}] ${d.answer}`, 12))
      .catch(()=>{ respEl.textContent = 'Error.'; });
  });
  el.addEventListener('mouseleave', e => { state.active = false; });
}

// ── CORE HEATMAP ─────────────────────────────────────────────────────────────
function renderCores(pfx,cores){
  const el=document.getElementById(pfx+'-cores');
  if(!el)return;
  if(!cores||!cores.length){el.innerHTML='<div style="color:var(--muted2);font-size:.52rem">No core data</div>';return;}
  el.innerHTML=cores.map((p,i)=>{
    const bg=coreColor(p);
    const textC=p>50?'rgba(255,255,255,.9)':'rgba(255,255,255,.4)';
    return `<div class="core-cell" style="background:${bg};color:${textC}" title="Core ${i}: ${p.toFixed(1)}%">${p.toFixed(0)}</div>`;
  }).join('');
}

// ── TEMPS ─────────────────────────────────────────────────────────────────────
function renderTemps(pfx,temps,throttle){
  const el=document.getElementById(pfx+'-temps-wrap');
  if(!el)return;
  if(!shouldRefreshSlow('temps_'+pfx, 6000))return;
  if(!temps||!temps.length){el.innerHTML='';return;}
  const c=throttle?`<span style="color:var(--red);font-size:.5rem;margin-left:6px">⚠ THROTTLE RISK</span>`:'';
  el.innerHTML=`<div class="section-title">THERMALS${c}</div>`+
    temps.map(([lbl,t])=>{
      const col=t>85?'var(--red)':t>75?'var(--amber)':'var(--text)';
      return `<div class="temp-row"><span class="temp-label">${lbl}</span><span class="temp-val" style="color:${col}">${t}°C</span></div>`;
    }).join('');
}

// ── PROCS ─────────────────────────────────────────────────────────────────────
function renderProcs(id,rows,isCpu,maxVal,machine,newProcs){
  const el=document.getElementById(id);
  if(!el)return;
  const sig=(rows||[]).map(r=>r[0]+r[1].toFixed(1)).join('|');
  if(el.dataset.sig===sig)return;
  el.dataset.sig=sig;
  el.innerHTML=(rows||[]).map(([name,val])=>{
    const pct=Math.min(100,(val/(maxVal||1))*100);
    const col=isCpu?tier(val).color:'var(--blue)';
    const isNew=(newProcs||[]).includes(name);
    const newBadge=isNew?`<span class="proc-badge new">NEW</span>`:'';
    return `<div class="proc-row"
      onclick="openSpotlight('${machine}','${name}',${isCpu?val:0},${isCpu?0:val})"
      oncontextmenu="event.preventDefault();openBiography('${machine}','${name}')"
      title="${name} — left click: spotlight · right click: biography">
      <div class="proc-name">${name}${newBadge}</div>
      <div class="proc-bar-wrap"><div class="proc-bar" style="width:${pct}%;background:${col}"></div></div>
      <div class="proc-val">${isCpu?val.toFixed(1)+'%':val.toFixed(0)+'MB'}</div>
    </div>`;
  }).join('');
}

// ── PM2 ───────────────────────────────────────────────────────────────────────
function renderPm2(d){
  if(!d||!d.pm2_available)return;
  const statusEl=document.getElementById('pm2-svc-status');
  if(statusEl){
    const st=d.cypher_online?'online':(d.critical_down&&d.critical_down.length?'degraded':'offline');
    statusEl.textContent=d.cypher_online?'UP':(d.critical_down&&d.critical_down.length?'DEGRADED':'OFFLINE');
    statusEl.className='pm2-svc-status '+st;
  }
  const alertsEl=document.getElementById('pm2-alerts');
  if(alertsEl)alertsEl.innerHTML=(d.pm2_alerts||[]).map(a=>
    `<div style="color:var(--red);font-size:.56rem;margin-bottom:3px;padding:2px 6px;border-left:2px solid var(--red);background:rgba(232,68,42,.04)">⚠ ${a}</div>`
  ).join('');
  const listEl=document.getElementById('pm2-proc-list');
  if(listEl){
    const _pm2sig=(d.pm2_processes||[]).map(p=>p.name+p.status+p.mem_mb.toFixed(0)+p.restarts).join('|');
    if(listEl.dataset.sig!==_pm2sig){
      listEl.dataset.sig=_pm2sig;
      listEl.innerHTML=(d.pm2_processes||[]).map(p=>{
        const upH=Math.floor(p.uptime_s/3600),upM=Math.floor((p.uptime_s%3600)/60);
        const upStr=p.status==='online'?`${upH}h${String(upM).padStart(2,'0')}m`:'—';
        const crashBadge=p.new_crashes>0?`<span style="color:var(--red);margin-left:3px">+${p.new_crashes}⚡</span>`:'';
        const memColor=p.mem_mb>200?'var(--amber)':p.mem_mb>500?'var(--red)':'var(--muted)';
        return `<div class="pm2-proc">
          <div class="pm2-dot ${p.status}"></div>
          <div class="pm2-name ${p.is_critical?'critical':''}">${p.name}${crashBadge}</div>
          <div style="color:var(--muted2);text-align:right;font-size:.54rem">${p.cpu.toFixed(1)}%</div>
          <div style="color:${memColor};text-align:right;font-size:.54rem">${p.mem_mb.toFixed(0)}mb</div>
          <div style="color:var(--muted2);text-align:right;font-size:.54rem">${upStr}</div>
          <div style="color:var(--muted2);text-align:right;font-size:.5rem">${p.restarts}✕</div>
        </div>`;
      }).join('');
    }
  }
}

// ── INCIDENTS ─────────────────────────────────────────────────────────────────
function renderIncidents(pfx,d){
  const el=document.getElementById(pfx+'-incidents');
  if(!el)return;
  if(!shouldRefreshSlow('incidents_'+pfx, 4000))return;
  const items=[...((d.active_anomalies||[]).map(a=>({
    urgency:8,ts:Date.now()/1000-a.duration_s,
    text:`${a.proc} ${a.metric.toUpperCase()} spike: ${a.peak.toFixed(0)} for ${a.duration_s}s`,
    tier:'urgent',active:true, type:'anomaly'
  }))),...((d.recent_events||[]).slice(-5).map(e=>({
    urgency:5,ts:e.started_at,
    text:`${e.proc_name} ${e.metric} peak ${e.peak_val?.toFixed(0)} — ${e.resolved?'resolved':'active'}`,
    tier:'elevated',active:!e.resolved, type:'event'
  })))];
  el.innerHTML=items.slice(0,8).map(item=>{
    const ts=item.ts?new Date(item.ts*1000).toLocaleTimeString('en',{hour12:false}):'';
    return `<div class="incident-card ${item.tier}">
      <span class="incident-ts">${ts}</span>
      ${item.text}
      <button class="incident-pin" onclick="openPin('${pfx}',${item.ts||0})">PIN</button>
      <button class="incident-pin" onclick="queryIncident('${pfx}',${item.ts||0});fetch('/api/pdcm/act?type=${item.type}')" style="margin-left:2px">ANALYZE</button>
    </div>`;
  }).join('') || '<div style="color:var(--muted2);font-size:.55rem">No incidents</div>';
}

// ── PRIORITY FEED ─────────────────────────────────────────────────────────────
const FEED_SKIP = new Set(['METRIC_UPDATE','metric_update','ATLAS_BREACH','atlas_breach']);
function renderFeed(feed){
  const el=document.getElementById('feed-items');
  if(!el)return;
  const filtered=(feed||[]).filter(f=>!FEED_SKIP.has(f.label||'') && !FEED_SKIP.has(f.type||''));
  if(filtered.length===0){
    el.innerHTML='<span style="font-size:.68rem;color:var(--muted2);">ALL SYSTEMS NOMINAL</span>';
    return;
  }
  el.innerHTML=filtered.slice(0,8).map(f=>{
    const ts=new Date(f.ts*1000).toLocaleTimeString('en',{hour12:false});
    return `<div class="feed-item ${f.tier||'elevated'}">[${ts}] ${f.label||f.type}</div>`;
  }).join('');
}

// ── RCA ────────────────────────────────────────────────────────────────────────
function renderRCA(pfx,rca){
  const el=document.getElementById(pfx+'-rca');
  if(!el||!rca)return;
  if(el.dataset.last===rca)return;
  el.dataset.last=rca;
  const lines=rca.split('\n').filter(l=>l.trim());
  el.innerHTML=lines.map(line=>{
    const hm=line.match(/HYPOTHESIS:\s*(.+?)\s*\|\s*CONFIDENCE:\s*(\d+)%\s*\|\s*ACTION:\s*(\w+)/i);
    if(hm){
      return `<div class="rca-item">
        ${hm[1]}
        <span class="rca-confidence"> ${hm[2]}%</span>
        <span class="rca-action">${hm[3].toUpperCase()}</span>
      </div>`;
    }
    return `<div class="rca-item">${line}</div>`;
  }).join('') || '<div style="color:var(--muted2);font-size:.55rem">No active anomalies to analyze</div>';
}

// ── SRE ────────────────────────────────────────────────────────────────────────
function renderSRE(pfx,pending){
  const panel=document.getElementById(pfx+'-sre');
  const actionsEl=document.getElementById(pfx+'-sre-actions');
  if(!panel||!actionsEl)return;
  if(!pending||!pending.length){panel.style.display='none';return;}
  panel.style.display='block';
  actionsEl.innerHTML=pending.map(a=>`
    <div class="sre-action">
      <span style="color:var(--text);flex:1">${a.desc}</span>
      <code style="color:var(--muted2);font-size:.52rem;margin-right:8px">${a.cmd}</code>
      <button class="sre-exec-btn" onclick="executeSRE('${pfx}','${a.id}')">EXECUTE</button>
    </div>
  `).join('');
}

// ── FORECASTS ────────────────────────────────────────────────────────────────
function renderForecasts(pfx,forecasts){
  const el=document.getElementById(pfx+'-forecasts');
  if(!el||!forecasts)return;
  let html='';
  for(const[metric,fc] of Object.entries(forecasts)){
    if(!fc||!fc.available)continue;
    const curr=fc.current||0,pred=fc.predicted||curr;
    const col=pred>85?'var(--red)':pred>70?'var(--amber)':'var(--green)';
    const arrow=fc.direction==='rising'?'↑':fc.direction==='falling'?'↓':'→';
    const eta=fc.minutes_to_90?`<span style="color:var(--red);margin-left:6px">→90% in ${fc.minutes_to_90}min</span>`:'';
    html+=`<div style="margin-bottom:6px">
      <div style="display:flex;justify-content:space-between;margin-bottom:2px;font-size:.56rem">
        <span style="color:var(--muted)">${metric.toUpperCase()}</span>
        <span style="color:${col}">${arrow} ${pred.toFixed(1)}% predicted${eta}</span>
      </div>
      <div class="forecast-bar">
        <div class="forecast-fill" style="width:${curr}%;background:${col}33"></div>
        <div class="forecast-predicted" style="left:${Math.min(pred,100)}%"></div>
        <div class="forecast-label">${curr.toFixed(1)}% now · conf ${(fc.confidence*100).toFixed(0)}%</div>
      </div>
    </div>`;
  }
  el.innerHTML=html||'<div style="color:var(--muted2);font-size:.55rem">Collecting forecast data…</div>';

  // Also update gauge forecast lines
  ['cpu','ram'].forEach(m=>{
    const fc=forecasts[m];
    const fEl=document.getElementById(pfx+'-'+m+'-forecast');
    if(!fEl||!fc||!fc.available)return;
    const arrow=fc.direction==='rising'?'↑':fc.direction==='falling'?'↓':'→';
    const col=fc.predicted>85?'var(--red)':fc.predicted>70?'var(--amber)':'var(--muted2)';
    const eta=fc.minutes_to_90?` · <span style="color:var(--red)">90% in ${fc.minutes_to_90}min</span>`:'';
    fEl.innerHTML=`<span style="color:${col}">${arrow} ${fc.predicted.toFixed(1)}% in 5min${eta}</span>`;
  });
}

// ── ANNOTATIONS ──────────────────────────────────────────────────────────────
function renderAnnotations(pfx,annotations){
  const el=document.getElementById(pfx+'-annotations');
  if(!el)return;
  if(!shouldRefreshSlow('annotations_'+pfx, 5000))return;
  const items=(annotations||[]).filter(a=>!a.machine||a.machine===pfx).slice(0,8);
  el.innerHTML=items.map(a=>{
    const ts=new Date(a.ts*1000).toLocaleTimeString('en',{hour12:false});
    const urg=a.urgency>=7?`style="color:var(--red)"`:'';
    return `<div class="annotation ${a.auto?'auto':''}">
      <span class="ann-ts">${ts}</span>
      <span ${urg}>${a.label}</span>
      ${a.body?`<div style="color:var(--muted2);font-size:.52rem;margin-top:2px">${a.body}</div>`:''}
    </div>`;
  }).join('') || '<div style="color:var(--muted2);font-size:.55rem">No annotations</div>';
}

// ── FLASH PROCS ──────────────────────────────────────────────────────────────
function renderFlashProcs(pfx,flashes){
  const row=document.getElementById(pfx+'-flash-row');
  const el=document.getElementById(pfx+'-flash-procs');
  if(!row||!el)return;
  const recent=(flashes||[]).filter(f=>Date.now()/1000-f.ts<30);
  row.style.display=recent.length?'block':'none';
  el.innerHTML=recent.map(f=>`<span class="flash-proc">${f.proc}</span>`).join('');
}

// ── VERDICT HISTORY ───────────────────────────────────────────────────────────
function renderHistory(pfx,hist){
  const el=document.getElementById(pfx+'-hist');
  if(!el)return;
  const sig=(hist||[]).map(h=>h.ts+h.score).join('|');
  if(el.dataset.sig===sig)return;
  el.dataset.sig=sig;
  el.innerHTML=(hist||[]).slice(-8).reverse().map(h=>`
    <div class="hist-row">
      <div class="hist-ts">${h.ts}</div>
      <div class="hist-text">${h.verdict}</div>
      <div class="hist-score" style="color:${scoreColor(h.score)}">${h.score}</div>
    </div>`).join('');
}

// ── SCORE ─────────────────────────────────────────────────────────────────────
function renderScore(pfx,d){
  const score=d.score||1000,grade=d.grade||'S';
  const col=scoreColor(score);
  const numEl=document.getElementById(pfx+'-score-num');
  const gradeEl=document.getElementById(pfx+'-score-grade');
  if(numEl){numEl.textContent=score;numEl.style.color=col;}
  if(gradeEl){gradeEl.textContent=grade;gradeEl.style.color=col;}
  if(pfx==='local'){scoreHistLocal.push(score);if(scoreHistLocal.length>60)scoreHistLocal.shift();drawScoreSpark('local-score-spark',scoreHistLocal);}
  else{scoreHistOracle.push(score);if(scoreHistOracle.length>60)scoreHistOracle.shift();drawScoreSpark('oracle-score-spark',scoreHistOracle);}
}

// ── GAUGE ─────────────────────────────────────────────────────────────────────
function updateGauge(pfx,key,pct,history,verdictText){
  const t=tier(pct);
  const pEl=document.getElementById(pfx+'-'+key+'-pct');
  const bEl=document.getElementById(pfx+'-'+key+'-bar');
  const tEl=document.getElementById(pfx+'-'+key+'-tier');
  const vEl=document.getElementById(pfx+'-'+key+'-verdict');
  if(pEl){pEl.textContent=pct.toFixed(1);pEl.style.color=t.color;}
  if(bEl){bEl.style.width=pct+'%';bEl.style.background=t.color;}
  if(tEl){tEl.textContent=t.label;tEl.style.color=t.color;tEl.style.border=`1px solid ${t.color}44`;}
  if(vEl&&verdictText&&vEl.textContent!==verdictText)vEl.textContent=verdictText;
  drawSpark(pfx+'-'+key+'-spark',history,t.color,key==='cpu'?85:90);
}

// ── TAMA ──────────────────────────────────────────────────────────────────────
function updateTama(pfx,mood,msg){
  const fEl=document.getElementById(pfx+'-tama-face');
  const mEl=document.getElementById(pfx+'-tama-msg');
  if(!fEl||!mEl)return;
  const faces=TAMA_FACES[mood]||TAMA_FACES.content;
  fEl.textContent=faces[Math.floor(Date.now()/3000)%faces.length];
  fEl.style.color=TAMA_COLORS[mood]||'var(--text)';
  if(msg&&msg!==mEl.dataset.last){mEl.dataset.last=msg;typewrite(mEl,msg,12);}
}

// ── NET ───────────────────────────────────────────────────────────────────────
function renderNet(pfx,conns){
  const el=document.getElementById(pfx+'-net');
  if(!el)return;
  if(!shouldRefreshSlow('net_'+pfx, 3000))return;
  el.innerHTML=(conns||[]).slice(0,8).map(c=>`
    <div class="net-row">
      <span class="net-proc">${c.proc}</span>
      <span class="net-addr">${c.raddr}</span>
      <span class="net-status ${c.status}">${c.status}</span>
    </div>`).join('') || '<div style="color:var(--muted2);font-size:.55rem">No connections</div>';
}

// ── MAIN UPDATE ───────────────────────────────────────────────────────────────
function updateMachine(pfx,d){
  if(!d)return;
  const online=d.online!==false;
  const sb=document.getElementById(pfx+'-status');
  if(sb){sb.textContent=online?'ONLINE':'OFFLINE';sb.className='status-badge '+(online?'online':'offline');}
  if(!online&&pfx==='oracle')return;

  updateGauge(pfx,'cpu',d.cpu_pct||0,d.cpu_history||[],d.cpu_verdict);
  updateGauge(pfx,'ram',d.ram_pct||0,d.ram_history||[],d.ram_verdict);
  // Attach drag-select to sparklines (idempotent — checks dataset.dragAttached)
  attachSparkDrag(pfx+'-cpu-spark', pfx, 'cpu', (d.cpu_history||[]).length);
  attachSparkDrag(pfx+'-ram-spark', pfx, 'ram', (d.ram_history||[]).length);
  updateTama(pfx,d.tamagotchi_mood||'happy',d.tamagotchi_msg);
  renderScore(pfx,d);
  renderCores(pfx,d.cpu_per_core||[]);
  renderTemps(pfx,d.cpu_temps,d.thermal_throttle_risk);
  renderIncidents(pfx,d);
  renderNet(pfx,d.net_connections);
  renderHistory(pfx,d.verdict_history);
  renderAnnotations(pfx,d.annotations);
  renderFlashProcs(pfx,d.flash_procs);

  const newProcs=d.new_procs||[];
  const cpuMax=(d.cpu_top||[]).length?Math.max(...d.cpu_top.map(x=>x[1])):1;
  const ramMax=(d.ram_top||[]).length?Math.max(...d.ram_top.map(x=>x[1])):1;
  renderProcs(pfx+'-cpu-procs',d.cpu_top,true,cpuMax,pfx,newProcs);
  renderProcs(pfx+'-ram-procs',d.ram_top,false,ramMax,pfx,newProcs);

  if(pfx==='oracle')renderPm2(d);

  renderRCA(pfx,d.root_cause);
  renderSRE(pfx,d.sre_pending);
  renderForecasts(pfx,d.forecasts);

  if(d.priority_feed)renderFeed(d.priority_feed);

  // AI pill
  ['',2].forEach(sfx=>{
    const pill=document.getElementById(pfx+'-ai-pill'+sfx);
    if(!pill)return;
    const labels={waiting:'STANDBY',fetching:'ACTIVE',ready:'LIVE',error:'LIMIT'};
    const labelColors={waiting:'waiting',fetching:'fetching',ready:'ready',error:'waiting'};
    pill.querySelector('span').textContent=(pfx==='local'?'LOCAL ':'ORACLE ')+(labels[d.ai_status]||'AI');
    pill.className='ai-pill '+(labelColors[d.ai_status]||'waiting');
  });

  // Meta
  const meta=document.getElementById(pfx+'-meta');
  if(meta){
    const up=d.session_start?Math.floor(Date.now()/1000-d.session_start):0;
    const h=Math.floor(up/3600),m=Math.floor((up%3600)/60),s=up%60;
    meta.textContent=`UP ${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')} · ${d.ai_call_count||0} AI CALLS · PEAK CPU ${(d.peak_cpu||0).toFixed(0)}%`;
  }

  // VIE directives — full panel expansion and dimming
  if(d.vie) applyVIEDirectives(pfx, d.vie);

  // Digest
  const dg=document.getElementById(pfx+'-digest');
  if(dg&&d.digest)dg.textContent=d.digest;
}

// ── NARRATIVE ─────────────────────────────────────────────────────────────────
let _narrativeLast='';
function updateNarrative(local,oracle){
  const nb=document.getElementById('narrative-bar');
  if(!nb)return;
  const text=(local?.cross_narrative||oracle?.cross_narrative||'').trim();
  if(!text||text===_narrativeLast)return;
  if(nb._twTimer)return;  // still typing — don't interrupt
  _narrativeLast=text;
  typewrite(nb,text,10);
}

// ── FETCH ─────────────────────────────────────────────────────────────────────
// fetchAll replaced by backoff version above

// ── FREEZE ───────────────────────────────────────────────────────────────────
function toggleFreeze(){
  frozen=!frozen;
  const btn=document.getElementById('freeze-btn');
  const overlay=document.getElementById('freeze-overlay');
  const label=document.getElementById('freeze-label');
  btn.classList.toggle('active',frozen);
  btn.textContent=frozen?'RESUME':'FREEZE';
  overlay.classList.toggle('active',frozen);
  label.classList.toggle('active',frozen);
}

// ── CMD ───────────────────────────────────────────────────────────────────────
async function sendCmd(){
  const q=document.getElementById('cmd-input').value.trim();
  const machine=document.getElementById('cmd-machine').value;
  if(!q)return;
  const resp=document.getElementById('cmd-resp');
  resp.style.display='block';resp.textContent='';
  typewrite(resp,'Thinking…',10);
  try{
    const r=await fetch(`/api/query?machine=${machine}&q=${encodeURIComponent(q)}`);
    const d=await r.json();
    resp.textContent='';
    typewrite(resp,`[${machine.toUpperCase()}] ${d.answer}`,12);
  }catch{resp.textContent='AI error.';}
}
document.getElementById('cmd-input').addEventListener('keydown',e=>{if(e.key==='Enter')sendCmd();});

// ── SPOTLIGHT ────────────────────────────────────────────────────────────────
async function openSpotlight(machine,name,cpu,ram){
  document.getElementById('proc-modal-title').textContent=name;
  document.getElementById('proc-modal-sub').textContent=`${machine.toUpperCase()} · CPU ${cpu.toFixed(1)}% · RAM ${ram.toFixed(0)}MB`;
  document.getElementById('proc-modal-body').textContent='Consulting AI…';
  openModal('proc-modal');
  try{
    const r=await fetch(`/api/spotlight?machine=${machine}&proc=${encodeURIComponent(name)}&cpu=${cpu}&ram=${ram}`);
    const d=await r.json();
    typewrite(document.getElementById('proc-modal-body'),d.answer,14);
  }catch{document.getElementById('proc-modal-body').textContent='AI error.';}
}

function openSpotlightPicker(machine){
  const d=lastData[machine];
  if(!d||!d.cpu_top)return;
  const [name,cpu]=d.cpu_top[0]||['unknown',0];
  openSpotlight(machine,name,cpu,0);
}

// ── POST-MORTEM ──────────────────────────────────────────────────────────────
async function triggerPostmortem(machine){
  document.getElementById('post-modal-sub').textContent=machine.toUpperCase()+' — last 60 seconds';
  document.getElementById('post-modal-body').textContent='Analyzing…';
  openModal('post-modal');
  try{
    const r=await fetch(`/api/postmortem?machine=${machine}`);
    const d=await r.json();
    typewrite(document.getElementById('post-modal-body'),d.answer,14);
  }catch{document.getElementById('post-modal-body').textContent='AI error.';}
}

// ── WHY SCORE ────────────────────────────────────────────────────────────────
async function whyScore(machine){
  document.getElementById('why-modal-sub').textContent=machine.toUpperCase();
  document.getElementById('why-modal-body').textContent='Analyzing session data…';
  openModal('why-modal');
  try{
    const r=await fetch(`/api/query?machine=${machine}&q=Why is my health score what it is? Be specific about processes and events.`);
    const d=await r.json();
    typewrite(document.getElementById('why-modal-body'),d.answer,14);
  }catch{document.getElementById('why-modal-body').textContent='AI error.';}
}

// ── INCIDENT QUERY ────────────────────────────────────────────────────────────
async function queryIncident(machine,ts){
  document.getElementById('incident-modal-sub').textContent=`${machine.toUpperCase()} · ${new Date(ts*1000).toLocaleTimeString()}`;
  document.getElementById('incident-modal-body').textContent='Reconstructing causal chain…';
  openModal('incident-modal');
  try{
    const r=await fetch(`/api/incident?machine=${machine}&ts=${ts}`);
    const d=await r.json();
    typewrite(document.getElementById('incident-modal-body'),d.answer,14);
  }catch{document.getElementById('incident-modal-body').textContent='Error.';}
}

// ── PIN ───────────────────────────────────────────────────────────────────────
function openPin(machine,ts){
  document.getElementById('pin-machine').value=machine+':'+ts;
  document.getElementById('pin-note').value='';
  openModal('pin-modal');
}

async function submitPin(){
  const parts=document.getElementById('pin-machine').value.split(':');
  const machine=parts[0],ts=parts[1];
  const note=document.getElementById('pin-note').value.trim()||'Pinned moment';
  try{await fetch(`/api/pin?machine=${machine}&ts=${ts}&note=${encodeURIComponent(note)}`);}catch{}
  closeModal('pin-modal');
}

// ── SRE EXECUTE ──────────────────────────────────────────────────────────────
async function executeSRE(machine,actionId){
  if(!confirm('Execute this repair action?'))return;
  try{
    const r=await fetch(`/api/sre?machine=${machine}&action=${actionId}`,{method:'POST'});
    const d=await r.json();
    alert(d.ok?'Executed: '+d.output:'Failed: '+d.error);
  }catch{alert('Execution error.');}
}

// ── TERMINAL ─────────────────────────────────────────────────────────────────
function openTerminal(machine){
  document.getElementById('terminal-output').innerHTML='';
  addTerminalLine('$ SYSWATCH V5 ORACLE TERMINAL', false);
  addTerminalLine('$ Connected to oracle via HTTP exec endpoint', false);
  openModal('terminal-modal');
}

function addTerminalLine(text,isErr){
  const el=document.getElementById('terminal-output');
  const line=document.createElement('div');
  line.className='t-line'+(isErr?' t-err':'');
  line.textContent=text;
  el.appendChild(line);
  el.scrollTop=el.scrollHeight;
}

async function sendTerminalCmd(){
  const inp=document.getElementById('terminal-input');
  const cmd=inp.value.trim();
  if(!cmd)return;
  addTerminalLine('$ '+cmd,false);
  inp.value='';
  try{
    const r=await fetch(`/api/exec?machine=oracle&cmd=${encodeURIComponent(cmd)}`);
    const d=await r.json();
    (d.output||'').split('\n').forEach(l=>addTerminalLine(l,false));
    if(d.error)addTerminalLine('ERR: '+d.error,true);
  }catch(e){addTerminalLine('Error: '+e,true);}
}
document.getElementById('terminal-input').addEventListener('keydown',e=>{if(e.key==='Enter')sendTerminalCmd();});

// ── EXPORT ───────────────────────────────────────────────────────────────────
function exportSession(){
  const data=JSON.stringify({
    timestamp:new Date().toISOString(),
    local:lastData.local,
    oracle:lastData.oracle
  },null,2);
  const blob=new Blob([data],{type:'application/json'});
  const a=document.createElement('a');
  a.href=URL.createObjectURL(blob);
  a.download=`syswatch-session-${new Date().toISOString().slice(0,19).replace(/:/g,'-')}.json`;
  a.click();
}

// ── MODAL HELPERS ─────────────────────────────────────────────────────────────
function openModal(id){document.getElementById(id).classList.add('open');}
function closeModal(id){document.getElementById(id).classList.remove('open');}

// ── CMD PALETTE ──────────────────────────────────────────────────────────────
const PALETTE_CMDS=[
  {label:'Ask AI about local',kbd:'',fn:()=>{document.getElementById('cmd-machine').value='local';document.getElementById('cmd-input').focus();}},
  {label:'Ask AI about oracle',kbd:'',fn:()=>{document.getElementById('cmd-machine').value='oracle';document.getElementById('cmd-input').focus();}},
  {label:'Post-mortem — local',kbd:'',fn:()=>triggerPostmortem('local')},
  {label:'Post-mortem — oracle',kbd:'',fn:()=>triggerPostmortem('oracle')},
  {label:'Why is my score low — local',kbd:'',fn:()=>whyScore('local')},
  {label:'Why is my score low — oracle',kbd:'',fn:()=>whyScore('oracle')},
  {label:'Open oracle terminal',kbd:'',fn:()=>openTerminal('oracle')},
  {label:'Export session JSON',kbd:'',fn:()=>exportSession()},
  {label:'Toggle freeze',kbd:'F',fn:()=>toggleFreeze()},
  {label:'Biography — top local process',kbd:'',fn:()=>{ const d=lastData.local; if(d?.cpu_top?.[0]) openBiography('local',d.cpu_top[0][0]); }},
  {label:'Biography — top oracle process',kbd:'',fn:()=>{ const d=lastData.oracle; if(d?.cpu_top?.[0]) openBiography('oracle',d.cpu_top[0][0]); }},
  {label:'Show heatmap',kbd:'',fn:()=>document.getElementById('heatmap-cal').scrollIntoView({behavior:'smooth'})},
  {label:'Session replay — load history',kbd:'',fn:()=>loadReplay()},
  {label:'Focus local machine',kbd:'1',fn:()=>focusColumn('local')},
  {label:'Focus oracle machine',kbd:'2',fn:()=>focusColumn('oracle')},
];

function openPalette(){
  document.getElementById('cmdpalette-bg').classList.add('open');
  document.getElementById('palette-input').value='';
  document.getElementById('palette-input').focus();
  renderPaletteList('');
}
function closePalette(){document.getElementById('cmdpalette-bg').classList.remove('open');}
function renderPaletteList(q){
  const el=document.getElementById('palette-list');
  const filtered=PALETTE_CMDS.filter(c=>!q||c.label.toLowerCase().includes(q.toLowerCase()));
  el.innerHTML=filtered.map((c,i)=>`
    <div class="palette-item" onclick="runPalette(${PALETTE_CMDS.indexOf(c)})">
      <span class="pi-label">${c.label}</span>
      ${c.kbd?`<span class="pi-kbd">${c.kbd}</span>`:''}
    </div>`).join('');
}
function runPalette(idx){PALETTE_CMDS[idx]?.fn();closePalette();}
document.getElementById('palette-input').addEventListener('input',e=>renderPaletteList(e.target.value));
document.getElementById('palette-input').addEventListener('keydown',e=>{if(e.key==='Escape')closePalette();});

// ── KEYBOARD SHORTCUTS ────────────────────────────────────────────────────────
function focusColumn(machine){
  focusMachine=focusMachine===machine?null:machine;
  document.getElementById('local-col').style.opacity=(!focusMachine||focusMachine==='local')?'1':'0.3';
  document.getElementById('oracle-col').style.opacity=(!focusMachine||focusMachine==='oracle')?'1':'0.3';
}
document.addEventListener('keydown',e=>{
  if(['INPUT','TEXTAREA'].includes(document.activeElement.tagName))return;
  if(e.key==='f'||e.key==='F')toggleFreeze();
  if(e.key==='1')focusColumn('local');
  if(e.key==='2')focusColumn('oracle');
  if(e.key==='q'||e.key==='Q')document.getElementById('cmd-input').focus();
  if((e.metaKey||e.ctrlKey)&&e.key==='k'){e.preventDefault();openPalette();}
  if(e.key==='Escape'){
    closePalette();
    ['proc-modal','post-modal','why-modal','incident-modal','pin-modal','terminal-modal']
      .forEach(id=>closeModal(id));
  }
});

// ── CLOCK ─────────────────────────────────────────────────────────────────────
function updateClock(){
  const n=new Date();
  document.getElementById('clock').textContent=
    n.toLocaleTimeString('en',{hour12:false})+' · '+n.toLocaleDateString('en',{weekday:'short',month:'short',day:'numeric'});
}


// ── FETCHALL ERROR BACKOFF ────────────────────────────────────────────────────
let _fetchErrors = 0;
async function fetchAll(){
  if(frozen) return;
  try {
    const r = await fetch('/api/state');
    if(!r.ok) throw new Error(r.status);
    const d = await r.json();
    _fetchErrors = 0;
    lastData = d;
    window._lastLocal  = d.local  || {};
    window._lastOracle = d.oracle || {};
    updateMachine('local', d.local);
    updateMachine('oracle', d.oracle);
    updateNarrative(d.local, d.oracle);
    const feed = [
      ...((d.local?.priority_feed)  || []),
      ...((d.oracle?.priority_feed) || [])
    ].sort((a,b) => b.ts - a.ts);
    renderFeed(feed.slice(0,10));
    renderFluxFeed(d.local, d.oracle);
    renderTheta(d.local?.theta || d.oracle?.theta);
    renderRunbook('local',  d.local);
    renderRunbook('oracle', d.oracle);
    document.getElementById('footer-ts').textContent =
      'LAST UPDATE ' + new Date().toLocaleTimeString('en',{hour12:false});
    updateDiffOverlay(d.local, d.oracle);
    if(_replay.playing) advanceReplay();
  } catch(e) {
    _fetchErrors++;
    console.warn('fetchAll error #' + _fetchErrors, e);
  }
}
// Exponential backoff: 1s → 2s → 4s → max 16s
function fetchInterval() {
  return Math.min(1000 * Math.pow(2, Math.max(0, _fetchErrors - 2)), 16000);
}
function scheduleNext() { setTimeout(() => { fetchAll().then(scheduleNext); }, fetchInterval()); }

// ── TIMELINE SCRUBBER ─────────────────────────────────────────────────────────
// Dual-machine scrubber — shows CPU+RAM history from SHARD as overlaid lines.
// Drag to select a time window, release to fire AI query on that window.
const _scrubState = {
  local: [], oracle: [],
  dragging: false, x0: 0, x1: 0,
  canvas: null, ctx: null,
  width: 0, height: 0,
};

async function initScrubber() {
  const wrap = document.getElementById('scrubber-wrap');
  if(!wrap) return;
  const canvas = document.createElement('canvas');
  canvas.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;cursor:crosshair';
  wrap.appendChild(canvas);
  _scrubState.canvas = canvas;
  _scrubState.ctx    = canvas.getContext('2d');
  resizeScrubber();
  window.addEventListener('resize', resizeScrubber);

  canvas.addEventListener('mousedown', e => {
    _scrubState.dragging = true;
    _scrubState.x0 = e.offsetX;
    _scrubState.x1 = e.offsetX;
    drawScrubber();
  });
  canvas.addEventListener('mousemove', e => {
    if(!_scrubState.dragging) return;
    _scrubState.x1 = e.offsetX;
    drawScrubber();
  });
  canvas.addEventListener('mouseup', e => {
    if(!_scrubState.dragging) return;
    _scrubState.dragging = false;
    const x0 = Math.min(_scrubState.x0, e.offsetX);
    const x1 = Math.max(_scrubState.x0, e.offsetX);
    if(x1 - x0 > 8) fireScrubberQuery(x0, x1);
  });
  await refreshScrubberData();
}

function resizeScrubber() {
  const c = _scrubState.canvas;
  if(!c) return;
  const dpr = window.devicePixelRatio || 1;
  const w = c.parentElement.offsetWidth;
  const h = c.parentElement.offsetHeight || 60;
  c.width  = w * dpr; c.height = h * dpr;
  c.style.width = w + 'px'; c.style.height = h + 'px';
  _scrubState.width  = w;
  _scrubState.height = h;
  _scrubState.ctx.scale(dpr, dpr);
  drawScrubber();
}

async function refreshScrubberData() {
  const since = Date.now()/1000 - 3600;
  try {
    const [rl, ro] = await Promise.all([
      fetch(`/api/history?machine=local&metric=cpu&since=${since}`).then(r=>r.json()),
      fetch(`/api/history?machine=oracle&metric=cpu&since=${since}`).then(r=>r.json()),
    ]);
    _scrubState.local  = rl.history  || [];
    _scrubState.oracle = ro.history  || [];
    drawScrubber();
  } catch(e) {}
  setTimeout(refreshScrubberData, 30000);
}

function drawScrubber() {
  const {ctx, width: W, height: H, local, oracle, dragging, x0, x1} = _scrubState;
  if(!ctx || !W) return;
  ctx.clearRect(0, 0, W, H);

  function drawLine(data, color) {
    if(!data || data.length < 2) return;
    const minTs = data[0].ts, maxTs = data[data.length-1].ts;
    const tspan = maxTs - minTs || 1;
    ctx.beginPath();
    data.forEach((p, i) => {
      const x = ((p.ts - minTs) / tspan) * W;
      const y = H - (p.value / 100) * H;
      i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
    });
    ctx.strokeStyle = color; ctx.lineWidth = 1.2; ctx.stroke();
  }

  drawLine(local,  '#2ae87a');
  drawLine(oracle, '#c9a84c');

  // Drag selection highlight
  if(dragging || (x0 !== x1)) {
    const lx = Math.min(x0, x1), rx = Math.max(x0, x1);
    ctx.fillStyle = 'rgba(201,168,76,.15)';
    ctx.fillRect(lx, 0, rx-lx, H);
    ctx.strokeStyle = 'rgba(201,168,76,.6)';
    ctx.lineWidth = 1;
    ctx.strokeRect(lx, 0, rx-lx, H);
  }

  // Legend
  ctx.font = '9px DM Mono, monospace';
  ctx.fillStyle = '#2ae87a'; ctx.fillText('LOCAL', 6, 12);
  ctx.fillStyle = '#c9a84c'; ctx.fillText('ORACLE', 46, 12);
  ctx.fillStyle = 'rgba(106,125,153,.6)'; ctx.fillText('CPU 1h', W-46, 12);
}

async function fireScrubberQuery(x0, x1) {
  const data = _scrubState.local.length ? _scrubState.local : _scrubState.oracle;
  if(!data.length) return;
  const minTs = data[0].ts, maxTs = data[data.length-1].ts;
  const tspan = maxTs - minTs || 1;
  const W = _scrubState.width;
  const tsStart = minTs + (x0 / W) * tspan;
  const tsEnd   = minTs + (x1 / W) * tspan;
  const midTs   = (tsStart + tsEnd) / 2;

  const respEl = document.getElementById('cmd-resp');
  respEl.style.display = 'block';
  respEl.textContent = '';
  typewrite(respEl, 'Analyzing selected time window…', 12);
  try {
    const r = await fetch(`/api/incident?machine=local&ts=${midTs}`);
    const d = await r.json();
    typewrite(respEl, `[SCRUBBER ${new Date(tsStart*1000).toLocaleTimeString()}–${new Date(tsEnd*1000).toLocaleTimeString()}] ${d.answer}`, 12);
  } catch(e) { respEl.textContent = 'Error.'; }
}

// ── DIFF OVERLAY CHART ────────────────────────────────────────────────────────
// Side-by-side LOCAL vs ORACLE CPU+RAM on a single canvas
let _diffCanvas = null;
let _diffCtx    = null;

function initDiffOverlay() {
  const wrap = document.getElementById('diff-overlay-wrap');
  if(!wrap) return;
  _diffCanvas = document.createElement('canvas');
  _diffCanvas.style.cssText = 'width:100%;height:100%;display:block;';
  wrap.appendChild(_diffCanvas);
  _diffCtx = _diffCanvas.getContext('2d');
  resizeDiffOverlay();
  window.addEventListener('resize', resizeDiffOverlay);
}

function resizeDiffOverlay() {
  if(!_diffCanvas) return;
  const dpr = window.devicePixelRatio || 1;
  const w = _diffCanvas.parentElement.offsetWidth || 400;
  const h = _diffCanvas.parentElement.offsetHeight || 80;
  _diffCanvas.width  = w * dpr; _diffCanvas.height = h * dpr;
  _diffCanvas.style.width = w+'px'; _diffCanvas.style.height = h+'px';
  _diffCtx.scale(dpr, dpr);
}

function updateDiffOverlay(local, oracle) {
  if(!_diffCtx || !_diffCanvas) return;
  const W = _diffCanvas.width / (window.devicePixelRatio||1);
  const H = _diffCanvas.height / (window.devicePixelRatio||1);
  const ctx = _diffCtx;
  ctx.clearRect(0,0,W,H);

  function drawOverlayLine(hist, color, label) {
    if(!hist || hist.length < 2) return;
    const pts = hist.map((v,i) => ({x: i/(hist.length-1)*W, y: H-(v/100)*H}));
    ctx.beginPath();
    pts.forEach((p,i) => i ? ctx.lineTo(p.x,p.y) : ctx.moveTo(p.x,p.y));
    ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.stroke();
    // Fill
    ctx.beginPath();
    ctx.moveTo(pts[0].x, H);
    pts.forEach(p => ctx.lineTo(p.x, p.y));
    ctx.lineTo(pts[pts.length-1].x, H);
    const g = ctx.createLinearGradient(0,0,0,H);
    const fillColor = color.length > 7 ? color : color+'44';
    g.addColorStop(0, fillColor); g.addColorStop(1,'transparent');
    ctx.fillStyle = g; ctx.fill();
    // Label
    ctx.font = '9px DM Mono, monospace';
    ctx.fillStyle = color;
    ctx.fillText(label + ' ' + hist[hist.length-1].toFixed(0)+'%', 6, label==='L-CPU'?12:22);
  }

  drawOverlayLine(local?.cpu_history,  '#2ae87a', 'L-CPU');
  drawOverlayLine(oracle?.cpu_history, '#c9a84c', 'O-CPU');
  drawOverlayLine(local?.ram_history,  '#3a8fd433', 'L-RAM');
  drawOverlayLine(oracle?.ram_history, '#9b6dff33', 'O-RAM');

  // Delta annotation
  if(local && oracle) {
    const dCpu = (local.cpu_pct||0) - (oracle.cpu_pct||0);
    const dRam = (local.ram_pct||0) - (oracle.ram_pct||0);
    ctx.font = '9px DM Mono, monospace';
    ctx.fillStyle = 'rgba(106,125,153,.8)';
    ctx.fillText(`ΔCPU ${dCpu>0?'+':''}${dCpu.toFixed(1)}%  ΔRAM ${dRam>0?'+':''}${dRam.toFixed(1)}%`, W-130, 12);
  }
}

// ── HEATMAP CALENDAR ─────────────────────────────────────────────────────────
// 24-column grid (hours of day) × N rows (past sessions from SHARD history)
// Color = avg CPU that hour. Click to query that hour.
async function renderHeatmapCalendar() {
  const el = document.getElementById('heatmap-cal');
  if(!el) return;
  try {
    const since = Date.now()/1000 - 86400; // last 24h
    const r = await fetch(`/api/history?machine=local&metric=cpu&since=${since}`);
    const d = await r.json();
    const rows = d.history || [];
    if(!rows.length) { el.innerHTML = '<div style="color:var(--muted2);font-size:.52rem">No history yet</div>'; return; }

    // Bucket by hour
    const buckets = {};
    rows.forEach(p => {
      const h = new Date(p.ts*1000).getHours();
      if(!buckets[h]) buckets[h] = [];
      buckets[h].push(p.value);
    });

    const cells = [];
    for(let h=0; h<24; h++) {
      const vals = buckets[h] || [];
      const avg  = vals.length ? vals.reduce((a,b)=>a+b,0)/vals.length : null;
      cells.push({h, avg});
    }

    el.style.display = 'grid';
    el.style.gridTemplateColumns = 'repeat(24,1fr)';
    el.style.gap = '2px';
    el.innerHTML = cells.map(({h, avg}) => {
      const bg  = avg === null ? 'var(--border)' : coreColor(avg);
      const tip = avg === null ? `${h}:00 — no data` : `${h}:00 avg ${avg.toFixed(0)}%`;
      return `<div style="height:18px;background:${bg};border-radius:2px;cursor:pointer;position:relative"
        title="${tip}"
        onclick="queryScrubHour(${h})"
      ><span style="position:absolute;bottom:1px;left:2px;font-size:9px;color:rgba(255,255,255,.4)">${h}</span></div>`;
    }).join('');
  } catch(e) {
    el.innerHTML = '<div style="color:var(--muted2);font-size:.52rem">Loading…</div>';
  }
}

async function queryScrubHour(hour) {
  const now   = new Date();
  const ts    = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hour, 30).getTime()/1000;
  const respEl = document.getElementById('cmd-resp');
  respEl.style.display = 'block';
  typewrite(respEl, `Analyzing ${String(hour).padStart(2,'0')}:00 window…`, 12);
  try {
    const r = await fetch(`/api/incident?machine=local&ts=${ts}`);
    const d = await r.json();
    typewrite(respEl, `[${String(hour).padStart(2,'0')}:00] ${d.answer}`, 12);
  } catch(e) { respEl.textContent = 'Error.'; }
}

// ── FLUX DIFF FEED ────────────────────────────────────────────────────────────
let _lastFluxSig = '';
async function renderFluxFeed(local, oracle) {
  if(!shouldRefreshSlow('flux_feed', 3000)) return;
  try {
    const [rl, ro] = await Promise.all([
      fetch('/api/flux?machine=local').then(r=>r.json()),
      fetch('/api/flux?machine=oracle').then(r=>r.json()),
    ]);
    const diffs = [
      ...(rl.diffs||[]).map(d=>({...d, machine:'local'})),
      ...(ro.diffs||[]).map(d=>({...d, machine:'oracle'})),
    ].sort((a,b)=>b.ts-a.ts).slice(0,12);

    const sig = diffs.map(d=>d.ts).join('|');
    if(sig === _lastFluxSig) return;
    _lastFluxSig = sig;

    const el = document.getElementById('flux-feed');
    if(!el) return;
    el.innerHTML = diffs.length ? diffs.map(d => {
      const ts  = new Date(d.ts*1000).toLocaleTimeString('en',{hour12:false});
      const changes = (d.changes||[]).map(c=>c.desc).join(' · ');
      const col = d.machine==='local' ? 'var(--teal)' : 'var(--gold)';
      return `<div style="font-size:.56rem;padding:2px 0;border-bottom:1px solid var(--border);display:flex;gap:6px;align-items:baseline">
        <span style="color:${col};font-size:.48rem;letter-spacing:.1em">${d.machine.toUpperCase()}</span>
        <span style="color:var(--muted2);font-size:.5rem">${ts}</span>
        <span style="color:var(--muted)">${changes}</span>
      </div>`;
    }).join('') : '<div style="color:var(--muted2);font-size:.55rem">No state changes yet</div>';
  } catch(e) {}
}

// ── ECHO RUNBOOK ─────────────────────────────────────────────────────────────
async function renderRunbook(pfx, d) {
  const el = document.getElementById(pfx+'-runbook');
  if(!el) return;
  if(!shouldRefreshSlow('runbook_'+pfx, 30000)) return;
  try {
    const r = await fetch(`/api/runbook?machine=${pfx}`);
    const rb = await r.json();
    if(rb.runbook) {
      el.style.display = 'block';
      const inner = document.getElementById(pfx+'-runbook-text');
      if(inner && inner.textContent !== rb.runbook) inner.textContent = rb.runbook;
    } else {
      el.style.display = 'none';
    }
  } catch(e) {}
}

// ── THETA CONTEXT ─────────────────────────────────────────────────────────────
function renderTheta(theta) {
  const el = document.getElementById('theta-bar');
  if(!el || !theta) return;
  const parts = [theta.day_str, theta.time_str];
  if(theta.is_night)    parts.push('NIGHT MODE');
  if(theta.is_weekend)  parts.push('WEEKEND');
  if(theta.is_business) parts.push('BUSINESS HOURS');
  const text = parts.join(' · ');
  if(el.textContent !== text) el.textContent = text;
}

// ── SESSION REPLAY ────────────────────────────────────────────────────────────
const _replay = {
  active:  false,
  frames:  [],
  idx:     0,
  playing: false,
};

async function loadReplay() {
  try {
    const since = Date.now()/1000 - 3600;
    const [rl, ro] = await Promise.all([
      fetch(`/api/history?machine=local&metric=cpu&since=${since}`).then(r=>r.json()),
      fetch(`/api/history?machine=oracle&metric=cpu&since=${since}`).then(r=>r.json()),
    ]);
    // Zip local+oracle by timestamp bucket (10s)
    const localRows  = rl.history || [];
    const oracleRows = ro.history || [];
    const buckets = {};
    localRows.forEach(p => {
      const k = Math.floor(p.ts/10)*10;
      if(!buckets[k]) buckets[k] = {ts:k, local_cpu:null, oracle_cpu:null};
      buckets[k].local_cpu = p.value;
    });
    oracleRows.forEach(p => {
      const k = Math.floor(p.ts/10)*10;
      if(!buckets[k]) buckets[k] = {ts:k, local_cpu:null, oracle_cpu:null};
      buckets[k].oracle_cpu = p.value;
    });
    _replay.frames = Object.values(buckets).sort((a,b)=>a.ts-b.ts);
    _replay.idx    = 0;
    updateReplayUI();
  } catch(e) {}
}

function updateReplayUI() {
  const slider = document.getElementById('replay-slider');
  const label  = document.getElementById('replay-label');
  const localBar  = document.getElementById('replay-local-bar');
  const oracleBar = document.getElementById('replay-oracle-bar');
  if(!slider) return;
  slider.max   = Math.max(0, _replay.frames.length - 1);
  slider.value = _replay.idx;
  const f = _replay.frames[_replay.idx];
  if(!f) return;
  const ts = new Date(f.ts*1000).toLocaleTimeString('en',{hour12:false});
  if(label) label.textContent = ts;
  if(localBar) {
    localBar.style.width = (f.local_cpu||0)+'%';
    localBar.style.background = tier(f.local_cpu||0).color;
  }
  if(oracleBar) {
    oracleBar.style.width = (f.oracle_cpu||0)+'%';
    oracleBar.style.background = tier(f.oracle_cpu||0).color;
  }
}

function advanceReplay() {
  if(!_replay.playing || !_replay.frames.length) return;
  _replay.idx = Math.min(_replay.idx + 1, _replay.frames.length - 1);
  updateReplayUI();
  if(_replay.idx >= _replay.frames.length - 1) _replay.playing = false;
}

function replayPlay() {
  _replay.playing = !_replay.playing;
  const btn = document.getElementById('replay-play-btn');
  if(btn) btn.textContent = _replay.playing ? '⏸ PAUSE' : '▶ PLAY';
}

function replayScrub(val) {
  _replay.idx = parseInt(val);
  updateReplayUI();
}

// ── VIE PANEL EXPANSION ───────────────────────────────────────────────────────
// When urgency ≥ 9: expand affected machine column, dim the other
function applyVIEDirectives(pfx, vie) {
  if(!vie) return;
  const col   = document.getElementById(pfx+'-col');
  const other = document.getElementById(pfx==='local'?'oracle-col':'local-col');
  if(!col) return;

  if(vie.alert_level === 'critical') {
    col.style.border   = '1px solid rgba(232,68,42,.35)';
    col.style.boxShadow = '0 0 20px rgba(232,68,42,.08) inset';
    if(other) other.style.opacity = '0.45';
    // Expand CPU gauge sparkline height
    const spark = document.getElementById(pfx+'-cpu-spark');
    if(spark) spark.style.height = '60px';
  } else if(vie.alert_level === 'urgent') {
    col.style.border   = '1px solid rgba(232,125,42,.25)';
    col.style.boxShadow = '';
    if(other) other.style.opacity = '0.75';
  } else {
    col.style.border   = '';
    col.style.boxShadow = '';
    if(other && !focusMachine) other.style.opacity = '1';
    const spark = document.getElementById(pfx+'-cpu-spark');
    if(spark) spark.style.height = '36px';
  }
}

// ── PROCESS BIOGRAPHY MODAL ──────────────────────────────────────────────────
async function openBiography(machine, proc) {
  document.getElementById('proc-modal-title').textContent = proc + ' — BIOGRAPHY';
  document.getElementById('proc-modal-sub').textContent   = machine.toUpperCase();
  document.getElementById('proc-modal-body').textContent  = 'Loading biography…';
  openModal('proc-modal');
  try {
    const r = await fetch(`/api/biography?machine=${machine}&proc=${encodeURIComponent(proc)}`);
    const d = await r.json();
    const known   = d.known ? 'KNOWN' : 'NEW';
    const rep     = d.reputation ? d.reputation.toFixed(1) : '?';
    const firstSeen = d.first_seen ?
      new Date(d.first_seen*1000).toLocaleString('en',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '?';
    const bio = [
      `PROCESS:    ${proc}`,
      `STATUS:     ${known}  ·  REPUTATION ${rep}/10`,
      `FIRST SEEN: ${firstSeen}`,
      `APPEARANCES:${d.appearances || 0}`,
      `AVG CPU:    ${d.avg_cpu || 0}%`,
      `AVG RAM:    ${d.avg_ram || 0} MB`,
      `INCIDENTS:  ${d.incidents || 0}`,
      '',
      '── AI ANALYSIS ──',
      d.ai || 'No AI analysis available.',
    ].join('\n');
    typewrite(document.getElementById('proc-modal-body'), bio, 10);
  } catch(e) {
    document.getElementById('proc-modal-body').textContent = 'Error loading biography.';
  }
}

// ── START ──────────────────────────────────────────────────────────────────
setInterval(updateClock, 1000);
updateClock();
initScrubber();
initDiffOverlay();
loadReplay();

// ResizeObserver — redraw all sparklines when layout changes
if(window.ResizeObserver) {
  const _sparkIds = [
    'local-cpu-spark','local-ram-spark','local-score-spark',
    'oracle-cpu-spark','oracle-ram-spark','oracle-score-spark'
  ];
  const _ro = new ResizeObserver(() => {
    if(lastData.local)  updateMachine('local',  lastData.local);
    if(lastData.oracle) updateMachine('oracle', lastData.oracle);
    resizeScrubber();
    resizeDiffOverlay();
    drawScrubber();
  });
  _ro.observe(document.querySelector('.main-grid') || document.body);
}
renderHeatmapCalendar();
setInterval(renderHeatmapCalendar, 300000);
scheduleNext();

// ── FLOOR PLAN ───────────────────────────────────────────────────────────────
const PERSON_COLORS = ['#2ae87a','#e8c97a','#9b6dff','#2addc9','#e87d2a','#e84a8a'];
let _personColorMap = {};
let _colorIdx = 0;

function personColor(label) {
  if (!_personColorMap[label]) {
    _personColorMap[label] = PERSON_COLORS[_colorIdx % PERSON_COLORS.length];
    _colorIdx++;
  }
  return _personColorMap[label];
}

// Floor Y centers in the SVG
const FLOOR_Y = {4: 57, 3: 137, 2: 210, 1: 298};
const FLOOR_X_START = 20;

function renderFloorPlan(phones) {
  // Clear all dot groups
  [1,2,3,4].forEach(f => {
    const g = document.getElementById(`floor${f}-dots`);
    if (g) g.innerHTML = '';
  });

  // Deduplicate by label — keep highest confidence
  const byLabel = {};
  phones.forEach(p => {
    if (!p.floor) return;
    const existing = byLabel[p.label];
    if (!existing || (p.confidence || 0) > (existing.confidence || 0)) {
      byLabel[p.label] = p;
    }
  });

  Object.values(byLabel).forEach((p, i) => {
    const color  = personColor(p.label);
    const floor  = p.floor;
    const g = document.getElementById(`floor${floor}-dots`);
    if (!g) return;
    const x = FLOOR_X_START + (i * 30);
    const y = FLOOR_Y[floor];
    const conf = p.confidence || 50;
    const methodIcon = p.method === 'bluetooth' ? '📶' : '📌';
    g.innerHTML += `
      <circle cx="${x}" cy="${y}" r="8" fill="${color}" opacity="${0.6 + conf/250}"
        style="filter:drop-shadow(0 0 5px ${color});cursor:pointer;"
        onclick="openFloorOverride('${p.label}','${p.label}')">
        <title>${p.label} · Floor ${floor} · ${conf}% conf · ${p.method||'bt'} ${p.rssi ? p.rssi+'dBm' : ''}</title>
      </circle>
      <text x="${x}" y="${y+4}" text-anchor="middle" fill="rgba(0,0,0,.9)"
        font-size="6" font-family="DM Mono,monospace" pointer-events="none">
        ${p.label.substring(0,2).toUpperCase()}
      </text>`;
  });
}

function openFloorOverride(identifier, label) {
  const floor = prompt(`Set floor for ${label} (1-4):`);
  if (floor && ['1','2','3','4'].includes(floor)) {
    // Try MAC-based override first, fall back to name-based
    fetch(`/api/systems/floor?mac=${encodeURIComponent(identifier)}&floor=${floor}`)
      .then(() => { fetchSystems(); fetchPalantir(); });
  }
}

// Systems state
let _sysData = {};
async function fetchSystems() {
  try {
    const r = await fetch('/api/systems');
    if (!r.ok) return;
    _sysData = await r.json();
    renderSystems(_sysData);
    // Re-render active tab if it needs systems data
    const activeTab = document.querySelector('.tab-btn.active')?.id?.replace('tab-','');
    if (activeTab === 'intelligence') renderIntelligence(_sysData, _palData);
    if (activeTab === 'network') renderNetworkTab();
  } catch {}
}

function renderSystems(s) {
  // Floor plan — use phone_locations from BT scan
  const phoneLocs = s.phone_locations || {};
  const palPhones = _palData?.devices?.phones || [];

  // Build combined list — BT located phones take priority
  const floorPhones = [];
  // Add BT-detected phones first
  Object.values(phoneLocs).forEach(p => {
    floorPhones.push({
      label: p.name,
      floor: p.floor,
      rssi:  p.rssi,
      method: 'bluetooth',
      confidence: p.rssi > -70 ? 90 : p.rssi > -80 ? 70 : 50,
    });
  });
  // Add presence-detected phones not in BT list
  palPhones.forEach(p => {
    const name = p.label || p.vendor;
    if (!phoneLocs[name]) {
      const manual = (s.floors || {})[p.mac];
      if (manual?.floor) {
        floorPhones.push({
          label: name, floor: manual.floor,
          method: 'manual', confidence: 100,
        });
      }
    }
  });

  renderFloorPlan(floorPhones);

  // Anchor status — show in palantir footer
  const anchors = s.anchors || {};
  const anchorEl = document.getElementById('pal-anchors');
  if (anchorEl) {
    const seen = Object.entries(anchors).filter(([,v]) => v.seen);
    anchorEl.textContent = `BT ANCHORS: ${seen.length}/${Object.keys(anchors).length} · DEVICES: ${s.bt_device_count || 0}`;
  }

  // VIGIL threats
  const threats = s.vigil_active || [];
  const vigilEl = document.getElementById('pal-alerts-row');
  if (vigilEl && threats.length > 0) {
    const extra = threats.map(t => {
      const color = t.score >= 8 ? 'var(--red)' : t.score >= 6 ? 'var(--amber)' : 'var(--muted)';
      return `<span style="font-size:.68rem;padding:3px 10px;border:1px solid ${color};
        color:${color};background:rgba(0,0,0,.3);">⚠ ${t.msg}</span>`;
    }).join('');
    vigilEl.innerHTML = extra + (vigilEl.innerHTML || '');
  }

  // Public IP
  if (s.public_ip) {
    const ipEl = document.getElementById('pal-pub-ip');
    if (ipEl) ipEl.textContent = s.public_ip;
  }

  // Vector directions
  const vectors = s.vectors || {};
  ['local_cpu','local_ram','oracle_cpu','oracle_ram'].forEach(k => {
    const v = vectors[k];
    if (!v) return;
    const parts = k.split('_');
    const pfx = parts[0], metric = parts[1];
    const el = document.getElementById(`${pfx}-${metric}-forecast`);
    if (el && v.trend !== 'stable') {
      const arrow = v.trend === 'rising' ? '↑' : '↓';
      const color = v.trend === 'rising' ? 'var(--amber)' : 'var(--teal)';
      el.innerHTML = `<span style="color:${color}">${arrow} ${Math.abs(v.velocity).toFixed(1)}%/min</span> ` + (el.innerHTML || '');
    }
  });

  // MOSAIC patterns
  const patterns = s.patterns || [];
  const patEl = document.getElementById('pal-patterns');
  if (patEl && patterns.length > 0) {
    patEl.innerHTML = patterns.slice(0,4).map(p =>
      `<div style="font-size:.68rem;padding:4px 8px;margin-bottom:3px;
        border-left:2px solid var(--purple);background:rgba(155,109,255,.04);color:var(--muted);">
        ${p.pattern} <span style="color:var(--muted2);">· ${p.occurrences}x · ${p.avg_delay_s}s delay</span>
      </div>`
    ).join('');
  }
}

// Init systems polling
fetchSystems();
setInterval(fetchSystems, 30000);

// ── TAB SYSTEM ────────────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + name)?.classList.add('active');
  document.getElementById('panel-' + name)?.classList.add('active');
  // Lazy-load tab content on first open
  if (name === 'intelligence') renderIntelligence(_sysData, _palData);
  if (name === 'network') renderNetworkTab();
  if (name === 'palantir-tab') renderPalantirTab();
}

// ── INTELLIGENCE TAB ──────────────────────────────────────────────────────────
function renderIntelligence(sys, pal) {
  // LENS focus
  const focus = sys?.focus;
  const lensScore = document.getElementById('lens-score');
  const lensDesc  = document.getElementById('lens-desc');
  if (lensScore) lensScore.textContent = focus ? Math.round(focus[1]?.score || 0) : '—';
  if (lensDesc)  lensDesc.textContent  = focus ? (focus[0] || 'All systems nominal') : 'Monitoring…';

  // VIGIL threats
  const vigilEl = document.getElementById('vigil-list');
  if (vigilEl) {
    const threats = sys?.vigil_active || [];
    vigilEl.innerHTML = threats.length === 0
      ? '<div style="font-size:.7rem;color:var(--muted2);">No active threats</div>'
      : threats.map(t => {
          const color = t.score >= 8 ? 'var(--red)' : t.score >= 6 ? 'var(--amber)' : 'var(--blue)';
          return `<div class="intel-item ${t.score>=8?'high':t.score>=6?'med':'low'}">
            <div class="intel-label">${t.type} · SCORE ${t.score}/10</div>
            ${t.msg}
          </div>`;
        }).join('');
  }

  // MOSAIC patterns
  const mosaicEl = document.getElementById('mosaic-list');
  if (mosaicEl) {
    const patterns = sys?.patterns || [];
    mosaicEl.innerHTML = patterns.length === 0
      ? '<div style="font-size:.7rem;color:var(--muted2);">Learning patterns… need more data</div>'
      : patterns.slice(0,5).map(p =>
          `<div class="intel-item low">
            <div class="intel-label">${p.occurrences}x · avg ${p.avg_delay_s}s apart · ${p.confidence}% conf</div>
            ${p.pattern}
          </div>`
        ).join('');
  }

  // CODEX recent
  const codexEl = document.getElementById('codex-list');
  if (codexEl) {
    const events = sys?.codex_recent || [];
    codexEl.innerHTML = events.length === 0
      ? '<div style="font-size:.7rem;color:var(--muted2);">No events recorded yet</div>'
      : events.slice(0,8).map(e => {
          const ts = new Date(e.ts*1000).toLocaleTimeString('en',{hour12:false,hour:'2-digit',minute:'2-digit'});
          return `<div class="scribe-entry">
            <span class="scribe-ts">${ts}</span>
            <span class="scribe-type" style="color:var(--muted2);">${e.category}</span>
            <span class="scribe-msg">${e.summary}</span>
          </div>`;
        }).join('');
  }

  // SCRIBE annotations — from local/oracle state
  const scribeEl = document.getElementById('scribe-list');
  if (scribeEl) {
    const anns = [...(window._lastLocal?.annotations||[]), ...(window._lastOracle?.annotations||[])]
      .sort((a,b) => (b.ts||0)-(a.ts||0));
    scribeEl.innerHTML = anns.length === 0
      ? '<div style="font-size:.7rem;color:var(--muted2);">No annotations yet</div>'
      : anns.slice(0,20).map(a => {
          const ts = new Date((a.ts||0)*1000).toLocaleTimeString('en',{hour12:false,hour:'2-digit',minute:'2-digit'});
          return `<div class="scribe-entry">
            <span class="scribe-ts">${ts}</span>
            <span class="scribe-type" style="color:${a.auto?'var(--muted2)':'var(--purple)'};">${a.type||'NOTE'}</span>
            <span class="scribe-msg">${a.text||a.label||''}</span>
          </div>`;
        }).join('');
  }

  // VECTOR directions
  const vectorEl = document.getElementById('vector-list');
  if (vectorEl) {
    const vectors = sys?.vectors || {};
    const names = {local_cpu:'Local CPU',local_ram:'Local RAM',oracle_cpu:'Oracle CPU',oracle_ram:'Oracle RAM'};
    vectorEl.innerHTML = Object.entries(vectors).map(([k,v]) => {
      const arrow = v.trend==='rising'?'↑':v.trend==='falling'?'↓':'→';
      const color = v.trend==='rising'?'var(--amber)':v.trend==='falling'?'var(--teal)':'var(--muted2)';
      return `<div style="display:flex;justify-content:space-between;padding:5px 0;
        border-bottom:1px solid var(--border);font-size:.73rem;">
        <span style="color:var(--muted);">${names[k]||k}</span>
        <span style="color:${color};">${arrow} ${Math.abs(v.velocity||0).toFixed(2)}%/min · ${v.trend}</span>
      </div>`;
    }).join('') || '<div style="font-size:.7rem;color:var(--muted2);">No vector data yet</div>';
  }

  // ATLAS forecasts from local/oracle state
  const atlasEl = document.getElementById('atlas-list');
  if (atlasEl) {
    const local  = window._lastLocal;
    const oracle = window._lastOracle;
    const items  = [];
    if (local?.cpu_forecast)  items.push({label:'Local CPU',  ...local.cpu_forecast});
    if (local?.ram_forecast)  items.push({label:'Local RAM',  ...local.ram_forecast});
    if (oracle?.cpu_forecast) items.push({label:'Oracle CPU', ...oracle.cpu_forecast});
    if (oracle?.ram_forecast) items.push({label:'Oracle RAM', ...oracle.ram_forecast});
    atlasEl.innerHTML = items.length === 0
      ? '<div style="font-size:.7rem;color:var(--muted2);">Forecasts building… need ~5min of data</div>'
      : items.map(f => `<div style="display:flex;justify-content:space-between;padding:5px 0;
          border-bottom:1px solid var(--border);font-size:.73rem;">
          <span style="color:var(--muted);">${f.label}</span>
          <span>${f.current}% → <span style="color:var(--amber);">${f.projected}%</span>
          <span style="color:var(--muted2);font-size:.65rem;"> in ${f.horizon_min}min · ${f.confidence}% conf</span></span>
        </div>`).join('');
  }

  // PDCM profile
  const pdcmEl = document.getElementById('pdcm-profile');
  if (pdcmEl && sys?.pdcm_profile) {
    const p = sys.pdcm_profile;
    const activeNow = p.active_now ? '<span style="color:var(--green);">ACTIVE NOW</span>' : '<span style="color:var(--muted2);">OFF-HOURS</span>';
    const peakHours = (p.peak_hours||[]).join('h, ')+'h';
    pdcmEl.innerHTML = `${activeNow} · Peak hours: ${peakHours||'learning…'}<br>
      Top watched procs: ${(p.top_procs||[]).map(([n])=>n).join(', ')||'none yet'}`;
  }
}

// ── NETWORK TAB ───────────────────────────────────────────────────────────────
function renderNetworkTab() {
  const local  = window._lastLocal;
  const oracle = window._lastOracle;
  const sys    = _sysData;

  // Local connections
  const localBody = document.getElementById('net-local-body');
  if (localBody && local?.net_connections) {
    const conns = local.net_connections.filter(c => c.status === 'ESTABLISHED').slice(0,20);
    localBody.innerHTML = conns.length === 0
      ? '<tr><td colspan="3" style="color:var(--muted2);padding:8px;">No active connections</td></tr>'
      : conns.map(c => `<tr>
          <td>${c.proc||'unknown'}</td>
          <td style="font-size:.65rem;color:var(--muted2);">${c.addr||''}</td>
          <td><span style="color:var(--green);font-size:.62rem;">●</span> ${c.status||''}</td>
        </tr>`).join('');
  }

  // Oracle connections
  const oracleBody = document.getElementById('net-oracle-body');
  if (oracleBody && oracle?.net_connections) {
    const conns = oracle.net_connections.filter(c => c.status === 'ESTABLISHED').slice(0,20);
    oracleBody.innerHTML = conns.length === 0
      ? '<tr><td colspan="3" style="color:var(--muted2);padding:8px;">No active connections</td></tr>'
      : conns.map(c => `<tr>
          <td>${c.proc||'unknown'}</td>
          <td style="font-size:.65rem;color:var(--muted2);">${c.addr||''}</td>
          <td><span style="color:var(--green);font-size:.62rem;">●</span> ${c.status||''}</td>
        </tr>`).join('');
  }

  // SENTINEL new connections
  const sentinelEl = document.getElementById('sentinel-new-conns');
  if (sentinelEl) {
    const newConns = sys?.new_connections || [];
    sentinelEl.innerHTML = newConns.length === 0
      ? '<span style="color:var(--muted2);">No new outbound connections detected</span>'
      : newConns.map(c => {
          const ts = new Date((c.ts||0)*1000).toLocaleTimeString('en',{hour12:false});
          return `<div style="padding:4px 0;border-bottom:1px solid var(--border);font-size:.7rem;">
            <span style="color:var(--amber);">NEW</span> · ${c.proc||'unknown'} → ${c.ip}
            <span style="color:var(--muted2);margin-left:8px;">${ts}</span>
          </div>`;
        }).join('');
  }

  // Stats
  const pubIp = document.getElementById('net-pub-ip');
  if (pubIp) pubIp.textContent = sys?.public_ip || '—';

  const devCount = document.getElementById('net-device-count');
  if (devCount) devCount.textContent = (sys?.bt_device_count || _palData?.devices?.total || '—') + ' devices';

  const btAnchors = document.getElementById('net-bt-anchors');
  if (btAnchors && sys?.anchors) {
    const seen = Object.values(sys.anchors).filter(a => a.seen).length;
    const total = Object.keys(sys.anchors).length;
    btAnchors.textContent = `${seen}/${total} visible`;
  }
}

// ── PALANTIR TAB ──────────────────────────────────────────────────────────────
function renderPalantirTab() {
  // Move palantir panel content into the tab
  const dest = document.getElementById('palantir-tab-content');
  const src  = document.getElementById('palantir-body');
  if (dest && src) {
    dest.innerHTML = '';
    dest.appendChild(src.cloneNode(true));
  }
}

// Store latest state for tab access
window._lastLocal  = {};
window._lastOracle = {};
let _palData = {};
let _labelModal = null;

function initPalantir() {
  // Build Palantir panel HTML
  const palHtml = `
<div id="palantir-panel" style="border-top:1px solid var(--border);background:var(--bg);">
  <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 24px;border-bottom:1px solid var(--border);cursor:pointer;" onclick="togglePalantir()">
    <div style="display:flex;align-items:center;gap:12px;">
      <span style="font-family:'Syncopate',sans-serif;font-size:.75rem;letter-spacing:.25em;color:var(--gold);">PALANTIR</span>
      <span id="pal-status" style="font-size:.65rem;color:var(--muted2);letter-spacing:.1em;">INITIALIZING</span>
    </div>
    <div style="display:flex;gap:16px;align-items:center;">
      <span id="pal-weather-mini" style="font-size:.72rem;color:var(--teal);"></span>
      <span id="pal-market-mini" style="font-size:.72rem;color:var(--muted);"></span>
      <span id="pal-toggle-hint" style="font-size:.62rem;color:var(--muted2);">▼ EXPAND</span>
    </div>
  </div>
  <div id="palantir-body" style="display:block;">
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:0;border-bottom:1px solid var(--border);">

      <!-- PRESENCE + FLOOR PLAN -->
      <div style="padding:16px 20px;border-right:1px solid var(--border);">
        <div class="section-title">PRESENCE · WHO'S HOME</div>
        <div id="pal-phones" style="margin-bottom:12px;"></div>

        <div class="section-title" style="margin-top:12px;">FLOOR PLAN</div>
        <div id="floor-plan" style="margin-bottom:10px;">
          <svg id="floor-svg" viewBox="0 0 260 340" style="width:100%;max-width:260px;display:block;">
            <!-- Floor 4 -->
            <rect x="2" y="2" width="256" height="78" rx="3" fill="rgba(10,13,21,.8)" stroke="#1a2540" stroke-width="1"/>
            <text x="10" y="16" fill="#4a5e7a" font-size="7" font-family="DM Mono,monospace" letter-spacing="1">FLOOR 4</text>
            <text x="10" y="30" fill="#6a7d99" font-size="7" font-family="DM Mono,monospace">Office</text>
            <rect x="8" y="35" width="72" height="40" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="12" y="50" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Bedroom 1</text>
            <rect x="84" y="35" width="72" height="40" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="88" y="50" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Bedroom 2</text>
            <rect x="160" y="35" width="94" height="40" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="164" y="50" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Office / Laundry</text>
            <g id="floor4-dots"></g>
            <!-- Floor 4 label badge -->
            <rect x="220" y="3" width="36" height="12" rx="2" fill="rgba(201,168,76,.1)" stroke="rgba(201,168,76,.3)"/>
            <text x="224" y="12" fill="#c9a84c" font-size="6" font-family="DM Mono,monospace">YOUR MAC</text>

            <!-- Floor 3 -->
            <rect x="2" y="84" width="256" height="78" rx="3" fill="rgba(10,13,21,.8)" stroke="#1a2540" stroke-width="1"/>
            <text x="10" y="98" fill="#4a5e7a" font-size="7" font-family="DM Mono,monospace" letter-spacing="1">FLOOR 3</text>
            <rect x="8" y="103" width="120" height="52" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="12" y="118" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Master Bedroom</text>
            <rect x="132" y="103" width="60" height="52" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="136" y="118" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Den</text>
            <rect x="196" y="103" width="58" height="52" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="200" y="118" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Bath</text>
            <g id="floor3-dots"></g>

            <!-- Floor 2 -->
            <rect x="2" y="166" width="256" height="78" rx="3" fill="rgba(10,13,21,.8)" stroke="#1a2540" stroke-width="1"/>
            <text x="10" y="180" fill="#4a5e7a" font-size="7" font-family="DM Mono,monospace" letter-spacing="1">FLOOR 2</text>
            <rect x="8" y="185" width="90" height="52" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="12" y="200" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Kitchen</text>
            <rect x="102" y="185" width="90" height="52" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="106" y="200" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Living Room</text>
            <rect x="196" y="185" width="58" height="52" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="200" y="200" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Dining</text>
            <g id="floor2-dots"></g>

            <!-- Floor 1 -->
            <rect x="2" y="248" width="256" height="88" rx="3" fill="rgba(10,13,21,.8)" stroke="#1a2540" stroke-width="1"/>
            <text x="10" y="262" fill="#4a5e7a" font-size="7" font-family="DM Mono,monospace" letter-spacing="1">FLOOR 1</text>
            <rect x="8" y="267" width="100" height="62" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="12" y="282" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Living Room</text>
            <rect x="112" y="267" width="80" height="62" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="116" y="282" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Guest Room</text>
            <rect x="196" y="267" width="58" height="62" rx="2" fill="rgba(26,37,64,.5)" stroke="#12192a"/>
            <text x="200" y="282" fill="#3a4a62" font-size="6" font-family="DM Mono,monospace">Bath</text>
            <g id="floor1-dots"></g>
          </svg>
        </div>

        <div class="section-title">ALL DEVICES</div>
        <div id="pal-devices" style="max-height:140px;overflow-y:auto;"></div>
        <div id="pal-unknown-alert" style="display:none;margin-top:8px;padding:6px 10px;
          border-left:3px solid var(--red);background:rgba(232,68,42,.06);font-size:.7rem;color:var(--red);"></div>
      </div>

      <!-- WEATHER + MARKETS -->
      <div style="padding:16px 20px;border-right:1px solid var(--border);">
        <div class="section-title">WEATHER</div>
        <div id="pal-weather" style="margin-bottom:16px;"></div>
        <div class="section-title">MARKETS</div>
        <div id="pal-markets"></div>
      </div>

      <!-- SPORTS -->
      <div style="padding:16px 20px;border-right:1px solid var(--border);">
        <div class="section-title">LIVE SCORES</div>
        <div id="pal-sports" style="max-height:320px;overflow-y:auto;"></div>
      </div>

      <!-- NEWS -->
      <div style="padding:16px 20px;">
        <div class="section-title">INTELLIGENCE FEED</div>
        <div id="pal-news" style="max-height:320px;overflow-y:auto;"></div>
      </div>

    </div>

    <!-- PRESENCE HISTORY + PATTERNS -->
    <div style="display:grid;grid-template-columns:1fr 1fr;border-bottom:1px solid var(--border);">
      <div style="padding:12px 24px;border-right:1px solid var(--border);">
        <div class="section-title">PRESENCE HISTORY</div>
        <div id="pal-history" style="display:flex;flex-wrap:wrap;gap:6px;"></div>
      </div>
      <div style="padding:12px 24px;">
        <div class="section-title">MOSAIC · DETECTED PATTERNS</div>
        <div id="pal-patterns"><span style="font-size:.68rem;color:var(--muted2);">Learning patterns…</span></div>
      </div>
    </div>

    <!-- SENTINEL + ALERTS -->
    <div style="padding:8px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:20px;">
      <span style="font-size:.65rem;color:var(--muted2);letter-spacing:.1em;">PUBLIC IP:</span>
      <span id="pal-pub-ip" style="font-size:.72rem;color:var(--teal);">detecting…</span>
      <span id="pal-anchors" style="font-size:.65rem;color:var(--muted2);margin-left:auto;"></span>
    </div>

    <!-- ALERTS -->
    <div id="pal-alerts-row" style="padding:8px 24px;display:flex;gap:8px;flex-wrap:wrap;min-height:32px;align-items:center;"></div>

  </div>
</div>

<!-- LABEL DEVICE MODAL -->
<div id="label-modal-bg" class="modal-bg">
  <div class="modal" style="max-width:420px;">
    <div class="modal-title">IDENTIFY DEVICE</div>
    <div class="modal-sub" id="label-modal-mac"></div>
    <div style="margin-bottom:12px;">
      <input id="label-input" type="text" placeholder="Enter name (e.g. Dad's iPhone)"
        style="width:100%;background:transparent;border:1px solid var(--border2);color:var(--text);
        padding:8px 12px;font-family:'DM Mono',monospace;font-size:.8rem;outline:none;">
    </div>
    <div style="display:flex;gap:8px;">
      <button class="modal-close" onclick="submitLabel()">SAVE</button>
      <button class="modal-close" onclick="closeModal('label-modal-bg')">CANCEL</button>
    </div>
  </div>
</div>`;

  // Append Palantir panel before footer
  const footer = document.querySelector('footer');
  if (footer) {
    footer.insertAdjacentHTML('beforebegin', palHtml);
  } else {
    document.body.insertAdjacentHTML('beforeend', palHtml);
  }
}

function togglePalantir() {
  const body = document.getElementById('palantir-body');
  const hint = document.getElementById('pal-toggle-hint');
  if (!body) return;
  const open = body.style.display !== 'none';
  body.style.display = open ? 'none' : 'block';
  hint.textContent = open ? '▼ EXPAND' : '▲ COLLAPSE';
}

async function fetchPalantir() {
  try {
    const r = await fetch('/api/palantir');
    if (!r.ok) return;
    _palData = await r.json();
    renderPalantir(_palData);
  } catch {}
}

function renderPalantir(d) {
  renderPresence(d.devices || {});
  renderWeather(d.weather || {});
  renderMarkets(d.markets || {});
  renderSports(d.sports || []);
  renderNews(d.news || []);
  renderPalAlerts(d.alerts || []);
  renderPresenceHistory(d.devices?.history || []);

  const statusEl = document.getElementById('pal-status');
  if (statusEl && d.last_updated) {
    const ts = new Date(d.last_updated * 1000);
    statusEl.textContent = 'LIVE · ' + ts.toLocaleTimeString('en-US', {hour:'2-digit',minute:'2-digit'});
    statusEl.style.color = 'var(--green)';
  }
}

function renderPresence(devices) {
  const phonesEl  = document.getElementById('pal-phones');
  const devicesEl = document.getElementById('pal-devices');
  const unknownEl = document.getElementById('pal-unknown-alert');
  if (!phonesEl) return;

  const phones  = devices.phones || [];
  const others  = devices.others || [];
  const unknown = devices.unknown_devices || [];

  // Mini header weather
  const mini = document.getElementById('pal-weather-mini');

  // Phones — people tracking
  if (phones.length === 0) {
    phonesEl.innerHTML = `<div style="font-size:.72rem;color:var(--muted2);">No phones detected</div>`;
  } else {
    phonesEl.innerHTML = phones.map(p => {
      const isKnown   = p.is_known || p.label !== p.vendor;
      const name      = p.label || p.vendor || 'Unknown';
      const since     = p.last_seen ? timeSince(p.last_seen) : '';
      const dot       = `<span style="width:9px;height:9px;border-radius:50%;background:var(--green);
        box-shadow:0 0 6px var(--green);display:inline-block;margin-right:8px;"></span>`;
      const floorData = (_sysData?.floors || {})[p.mac];
      const floorStr  = floorData?.floor ? `· Floor ${floorData.floor}` : '· Away';
      const floorBtn  = `<button onclick="openFloorOverride('${p.mac}','${name}')"
        style="background:transparent;border:1px solid var(--border2);color:var(--muted2);
        padding:1px 6px;font-size:.58rem;font-family:'DM Mono',monospace;cursor:pointer;margin-left:5px;">
        ${floorData?.floor ? `FL${floorData.floor}` : 'SET FLOOR'}</button>`;
      const labelBtn  = `<button onclick="openLabelModal('${p.mac}')"
        style="background:transparent;border:1px solid var(--border2);color:var(--muted2);
        padding:1px 7px;font-size:.6rem;font-family:'DM Mono',monospace;cursor:pointer;margin-left:6px;">
        ${isKnown ? 'RENAME' : 'IDENTIFY'}</button>`;
      return `<div style="display:flex;align-items:center;justify-content:space-between;
        padding:6px 0;border-bottom:1px solid var(--border);">
        <div>${dot}<span style="font-size:.78rem;font-weight:500;color:${isKnown ? 'var(--text)' : 'var(--amber)'};">${name}</span>${labelBtn}</div>
        <div style="font-size:.65rem;color:var(--muted2);">${p.ip} · ${since}</div>
      </div>`;
    }).join('');
  }

  // All other devices (non-phone, non-broadcast)
  const devList = others.filter(d => d.device_type !== 'broadcast' && d.device_type !== 'router');
  devicesEl.innerHTML = devList.map(d => {
    const icon = {tv:'📺', laptop:'💻', gaming:'🎮', iot:'📡', unknown:'❓'}[d.device_type] || '📱';
    const name = d.label || d.hostname || d.vendor || d.mac;
    const labelBtn = d.device_type === 'unknown'
      ? `<button onclick="openLabelModal('${d.mac}')"
          style="background:transparent;border:1px solid var(--border2);color:var(--muted2);
          padding:1px 6px;font-size:.58rem;font-family:'DM Mono',monospace;cursor:pointer;margin-left:5px;">ID</button>`
      : '';
    return `<div style="display:flex;align-items:center;justify-content:space-between;
      padding:4px 0;border-bottom:1px solid var(--border);font-size:.7rem;">
      <span>${icon} <span style="color:var(--muted);">${name}</span>${labelBtn}</span>
      <span style="color:var(--muted2);font-size:.65rem;">${d.ip}</span>
    </div>`;
  }).join('') || `<div style="font-size:.7rem;color:var(--muted2);">No other devices</div>`;

  // Unknown device alert
  if (unknown.length > 0) {
    unknownEl.style.display = 'block';
    unknownEl.innerHTML = `⚠ ${unknown.length} unidentified device${unknown.length>1?'s':''} on network — <a href="#" onclick="togglePalantir()" style="color:var(--red);">view</a>`;
  } else {
    unknownEl.style.display = 'none';
  }

  // Update total count status
  const total = (devices.total || 0);
  const statusEl = document.getElementById('pal-status');
  if (statusEl) {
    statusEl.textContent = `LIVE · ${phones.length} phone${phones.length!==1?'s':''} · ${total} devices`;
  }
}

function renderWeather(w) {
  const el   = document.getElementById('pal-weather');
  const mini = document.getElementById('pal-weather-mini');
  if (!el || !w.temp_f) return;

  const codeEmoji = w.code <= 1 ? '☀️' : w.code <= 3 ? '⛅' : w.code <= 48 ? '🌫️' :
    w.code <= 67 ? '🌧️' : w.code <= 77 ? '❄️' : w.code <= 82 ? '🌦️' : '⛈️';

  el.innerHTML = `
    <div style="display:flex;align-items:baseline;gap:10px;margin-bottom:6px;">
      <span style="font-size:2.2rem;font-family:'Syncopate',sans-serif;color:var(--teal);">${w.temp_f}°</span>
      <span style="font-size:1rem;">${codeEmoji}</span>
    </div>
    <div style="font-size:.72rem;color:var(--muted);margin-bottom:3px;">${w.condition}</div>
    <div style="font-size:.68rem;color:var(--muted2);">
      💨 ${w.wind_mph} mph &nbsp;·&nbsp; 💧 ${w.humidity}% humidity
    </div>
    <div style="font-size:.62rem;color:var(--muted2);margin-top:4px;">${w.city} · Updated ${w.updated}</div>`;

  if (mini) mini.textContent = `${codeEmoji} ${w.temp_f}°F`;
}

function renderMarkets(markets) {
  const el   = document.getElementById('pal-markets');
  const mini = document.getElementById('pal-market-mini');
  if (!el || !markets) return;

  const items = Object.values(markets);
  if (items.length === 0) {
    el.innerHTML = `<div style="font-size:.7rem;color:var(--muted2);">Loading market data…</div>`;
    return;
  }

  el.innerHTML = items.map(m => {
    const color = m.up ? 'var(--green)' : 'var(--red)';
    const arrow = m.up ? '▲' : '▼';
    const pct   = (m.change_pct >= 0 ? '+' : '') + m.change_pct.toFixed(2) + '%';
    return `<div style="display:flex;justify-content:space-between;align-items:center;
      padding:5px 0;border-bottom:1px solid var(--border);font-size:.72rem;">
      <span style="color:var(--muted);font-weight:500;">${m.name}</span>
      <div style="text-align:right;">
        <span style="color:var(--text);">${m.price.toLocaleString(undefined,{maximumFractionDigits:2})}</span>
        <span style="color:${color};margin-left:8px;font-size:.65rem;">${arrow} ${pct}</span>
      </div>
    </div>`;
  }).join('');

  // Mini bar — just S&P and BTC
  const sp  = markets['^GSPC'];
  const btc = markets['BTC-USD'];
  if (mini && sp) {
    const spStr  = `S&P ${sp.up ? '▲' : '▼'}${Math.abs(sp.change_pct).toFixed(1)}%`;
    const btcStr = btc ? ` · BTC ${btc.up ? '▲' : '▼'}${Math.abs(btc.change_pct).toFixed(1)}%` : '';
    mini.textContent = spStr + btcStr;
    mini.style.color = sp.up ? 'var(--green)' : 'var(--red)';
  }
}

function renderSports(games) {
  const el = document.getElementById('pal-sports');
  if (!el) return;
  if (games.length === 0) {
    el.innerHTML = `<div style="font-size:.7rem;color:var(--muted2);">No games right now</div>`;
    return;
  }
  el.innerHTML = games.map(g => {
    const live  = g.live;
    const dot   = live
      ? `<span style="width:7px;height:7px;border-radius:50%;background:var(--red);
          display:inline-block;margin-right:6px;animation:pulse .8s infinite;"></span>`
      : `<span style="width:7px;height:7px;border-radius:50%;background:var(--muted2);
          display:inline-block;margin-right:6px;"></span>`;
    const scoreStr = (g.home_score !== '' && g.away_score !== '')
      ? `<span style="font-family:'Syncopate',sans-serif;font-size:.7rem;color:var(--text);">
          ${g.away_score} – ${g.home_score}</span>`
      : '';
    return `<div style="padding:7px 0;border-bottom:1px solid var(--border);">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:2px;">
        <div>${dot}<span style="font-size:.65rem;color:var(--muted2);letter-spacing:.08em;">${g.league}</span></div>
        <span style="font-size:.62rem;color:${live?'var(--red)':'var(--muted2)'};">${g.status}</span>
      </div>
      <div style="display:flex;justify-content:space-between;align-items:center;padding-left:13px;">
        <span style="font-size:.76rem;">${g.away} <span style="color:var(--muted2);">@</span> ${g.home}</span>
        ${scoreStr}
      </div>
    </div>`;
  }).join('');
}

function renderNews(news) {
  const el = document.getElementById('pal-news');
  if (!el) return;
  if (news.length === 0) {
    el.innerHTML = `<div style="font-size:.7rem;color:var(--muted2);">Loading feed…</div>`;
    return;
  }
  const catColor = {
    top:'var(--gold)', markets:'var(--green)', tech:'var(--blue)',
    world:'var(--purple)', business:'var(--teal)'
  };
  el.innerHTML = news.map(n => {
    const color = catColor[n.source] || 'var(--muted2)';
    return `<div style="padding:6px 0;border-bottom:1px solid var(--border);cursor:pointer;"
      onclick="window.open('${n.url}','_blank')">
      <div style="font-size:.62rem;color:${color};letter-spacing:.08em;margin-bottom:2px;">
        ${n.source.toUpperCase()}</div>
      <div style="font-size:.72rem;color:var(--text);line-height:1.5;">${n.headline}</div>
    </div>`;
  }).join('');
}

function renderPresenceHistory(history) {
  const el = document.getElementById('pal-history');
  if (!el) return;
  const recent = history.slice(-20).reverse();
  if (recent.length === 0) {
    el.innerHTML = `<span style="font-size:.68rem;color:var(--muted2);">No events yet</span>`;
    return;
  }
  el.innerHTML = recent.map(ev => {
    const color = ev.event === 'joined' ? 'var(--green)' : 'var(--muted2)';
    const icon  = ev.event === 'joined' ? '→' : '←';
    const ts    = new Date(ev.ts * 1000).toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'});
    return `<span style="font-size:.65rem;padding:2px 8px;border:1px solid var(--border2);
      color:${color};background:rgba(0,0,0,.2);">
      ${icon} ${ev.label} <span style="color:var(--muted2);">${ts}</span>
    </span>`;
  }).join('');
}

function renderPalAlerts(alerts) {
  const el = document.getElementById('pal-alerts-row');
  if (!el) return;
  const recent = alerts.slice(0, 8);
  if (recent.length === 0) {
    el.innerHTML = `<span style="font-size:.65rem;color:var(--muted2);letter-spacing:.08em;">NO PALANTIR ALERTS</span>`;
    return;
  }
  el.innerHTML = recent.map(a => {
    const color = a.urgency >= 7 ? 'var(--red)' : a.urgency >= 5 ? 'var(--amber)' : 'var(--muted)';
    const ts    = new Date(a.ts * 1000).toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'});
    return `<span style="font-size:.68rem;padding:3px 10px;border:1px solid;color:${color};
      border-color:${color};background:rgba(0,0,0,.3);">
      ${a.msg} <span style="opacity:.6;">${ts}</span>
    </span>`;
  }).join('');
}

// Label modal
let _labelTargetMac = '';
function openLabelModal(mac) {
  _labelTargetMac = mac;
  document.getElementById('label-modal-mac').textContent = 'MAC: ' + mac;
  document.getElementById('label-input').value = '';
  document.getElementById('label-modal-bg').classList.add('open');
  document.getElementById('label-input').focus();
}

async function submitLabel() {
  const label = document.getElementById('label-input').value.trim();
  if (!label || !_labelTargetMac) return;
  try {
    await fetch(`/api/palantir/label?mac=${encodeURIComponent(_labelTargetMac)}&label=${encodeURIComponent(label)}`);
    closeModal('label-modal-bg');
    fetchPalantir();
  } catch {}
}

function timeSince(ts) {
  const sec = Math.floor(Date.now()/1000 - ts);
  if (sec < 60)  return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec/60)}m ago`;
  return `${Math.floor(sec/3600)}h ago`;
}

// Init and poll — panel is now static HTML, no dynamic injection needed
fetchPalantir();
setInterval(fetchPalantir, 30000);
</script>
</body>
</html>"""

# ── HTTP HANDLER ────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send_json(self, obj, status=200):
        data = json.dumps(obj, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        p  = urlparse(self.path)
        qs = parse_qs(p.query)

        if p.path in ("/", ""):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(HTML.encode())

        elif p.path == "/api/state":
            self.send_json({
                "local":  serialise(local_state),
                "oracle": serialise(oracle_state),
            })

        elif p.path == "/api/query":
            machine = qs.get("machine",["local"])[0]
            q       = qs.get("q",[""])[0]
            st      = local_state if machine == "local" else oracle_state
            self.send_json({"answer": handle_nl_query(st, q)})

        elif p.path == "/api/postmortem":
            machine = qs.get("machine",["local"])[0]
            st      = local_state if machine == "local" else oracle_state
            snap    = _local_snap if machine == "local" else _oracle_snap
            with _snap_lock:
                snap_copy = dict(snap)
            self.send_json({"answer": handle_postmortem(st, snap_copy)})

        elif p.path == "/api/spotlight":
            machine = qs.get("machine",["local"])[0]
            proc    = qs.get("proc",[""])[0]
            cpu     = float(qs.get("cpu",[0])[0])
            ram     = float(qs.get("ram",[0])[0])
            st      = local_state if machine == "local" else oracle_state
            self.send_json({"answer": handle_spotlight(st, proc, cpu, ram)})

        elif p.path == "/api/incident":
            machine = qs.get("machine",["local"])[0]
            ts      = qs.get("ts",["0"])[0]
            st      = local_state if machine == "local" else oracle_state
            self.send_json({"answer": handle_incident_query(st, ts)})

        elif p.path == "/api/pin":
            machine = qs.get("machine",["local"])[0]
            ts      = float(qs.get("ts",[time.time()])[0])
            note    = qs.get("note",["Pinned"])[0]
            helix   = local_helix if machine == "local" else oracle_helix
            helix.scribe.write(machine, note, auto=False, urgency=0)
            self.send_json({"ok": True})

        elif p.path == "/api/biography":
            machine = qs.get("machine",["local"])[0]
            proc    = qs.get("proc",[""])[0]
            helix   = local_helix if machine == "local" else oracle_helix
            bio     = helix.trace.get_biography(proc)
            # Enrich with AI spotlight
            st = local_state if machine == "local" else oracle_state
            ai_text = handle_spotlight(st, proc,
                bio.get("avg_cpu", 0), bio.get("avg_ram", 0))
            self.send_json({**bio, "ai": ai_text})

        elif p.path == "/api/flux":
            machine = qs.get("machine",["local"])[0]
            helix   = local_helix if machine == "local" else oracle_helix
            self.send_json({"diffs": helix.flux.get_recent(20)})

        elif p.path == "/api/runbook":
            machine  = qs.get("machine",["local"])[0]
            helix    = local_helix if machine == "local" else oracle_helix
            st       = local_state if machine == "local" else oracle_state
            runbook  = helix.echo.get_runbook(
                st.get("peak_cpu",0), st.get("peak_ram",0)
            )
            self.send_json({"runbook": runbook})

        elif p.path == "/api/history":
            machine = qs.get("machine",["local"])[0]
            metric  = qs.get("metric",["cpu"])[0]
            since   = float(qs.get("since",[time.time()-3600])[0])
            helix   = local_helix if machine == "local" else oracle_helix
            rows    = helix.shard.get_history(machine, metric, since)
            self.send_json({"history": rows})

        elif p.path == "/api/exec":
            machine = qs.get("machine",["oracle"])[0]
            cmd     = qs.get("cmd",["echo ok"])[0]
            if machine == "oracle" and oracle_url:
                try:
                    with urlopen(f"{oracle_url}/exec?cmd={cmd}", timeout=10) as r:
                        out = r.read().decode()
                    self.send_json({"output": out})
                except Exception as e:
                    self.send_json({"output": "", "error": str(e)})
            else:
                try:
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
                    self.send_json({"output": result.stdout + result.stderr})
                except Exception as e:
                    self.send_json({"output": "", "error": str(e)})
        elif p.path == "/api/pdcm/act":
            alert_type = qs.get("type",["unknown"])[0]
            systems.pdcm.record_alert_acted(alert_type)
            self.send_json({"ok": True})

        elif p.path == "/api/pdcm/dismiss":
            alert_type = qs.get("type",["unknown"])[0]
            systems.pdcm.record_alert_dismissed(alert_type)
            self.send_json({"ok": True})

        elif p.path == "/api/pdcm/proc":
            proc = qs.get("name",[""])[0]
            if proc:
                systems.pdcm.record_proc_view(proc)
            self.send_json({"ok": True})

        elif p.path == "/api/systems":
            self.send_json(systems.get_full_state())

        elif p.path == "/api/systems/floor":
            mac   = qs.get("mac",[""])[0]
            floor = int(qs.get("floor",["1"])[0])
            systems.meridian.set_override(mac, floor)
            self.send_json({"ok": True})

        elif p.path == "/api/palantir":
            self.send_json(palantir.get_state())

        elif p.path == "/api/palantir/label":
            mac   = qs.get("mac",[""])[0]
            label = qs.get("label",[""])[0]
            self.send_json(palantir.label_device(mac, label))

        elif p.path == "/api/palantir/history":
            self.send_json({"history": palantir.get_presence_history()})

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        from urllib.parse import urlparse, parse_qs
        p  = urlparse(self.path)
        qs = parse_qs(p.query)
        if p.path == "/api/sre":
            machine   = qs.get("machine",["local"])[0]
            action_id = qs.get("action",[""])[0]
            result    = handle_execute_sre(machine, action_id)
            self.send_json(result)
        else:
            self.send_response(404)
            self.end_headers()

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    global client, quiet, oracle_url

    parser = argparse.ArgumentParser()
    parser.add_argument("--key",    default=os.environ.get("GROQ_API_KEY",""))
    parser.add_argument("--oracle", default=None)
    parser.add_argument("--quiet",  action="store_true")
    parser.add_argument("--port",   default=PORT, type=int)
    args = parser.parse_args()

    quiet      = args.quiet
    oracle_url = args.oracle

    if args.key and not quiet:
        client = Groq(api_key=args.key)

    # Bind server FIRST before anything else can block
    import socket as _socket
    server = HTTPServer(("127.0.0.1", args.port), Handler)
    server.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)

    print(f"\n  ⬡ SYSWATCH V5  —  http://localhost:{args.port}")
    print(f"  Local:  collecting live")
    print(f"  Oracle: {oracle_url or 'not connected (use --oracle http://AGENT_IP:8766)'}")
    print(f"  AI:     {'disabled (quiet mode)' if quiet else 'adaptive · HELIX active'}")
    print(f"  DB:     ~/.syswatch_v5.db")
    print(f"  Ctrl+C to stop\n")

    # Start all background threads AFTER server is bound
    def _local_post_tick(s, i, t):
        _apply_smooth(s, "local")
        local_helix.process_tick(s)
        local_state.update(local_helix.get_export())

    threading.Thread(target=core.collect_loop,
                     args=(local_state, local_intel, _local_post_tick),
                     daemon=True).start()

    # Palantir + systems in a deferred thread so they never block server startup
    def _deferred_init():
        try: palantir.start()
        except Exception: pass
        try: systems._start_bt_scanner()
        except Exception: pass

    threading.Thread(target=_deferred_init, daemon=True).start()

    threading.Thread(target=ai_loop_for, args=(local_state, local_helix, local_intel), daemon=True).start()
    threading.Thread(target=snapshot_loop, daemon=True).start()
    threading.Thread(target=narrative_loop, daemon=True).start()

    if oracle_url:
        threading.Thread(target=oracle_poll_loop, daemon=True).start()
        threading.Thread(target=ai_loop_for, args=(oracle_state, oracle_helix, oracle_intel), daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped. Session saved to DB.\n")
        local_intel["scoring"].flush(local_state.get("score",1000), local_state["peak_cpu"], local_state["peak_ram"])
        local_helix.flush_session(local_state["peak_cpu"], local_state["peak_ram"],
            sum(p.get("restarts",0) for p in local_state.get("pm2_processes",[])))
        if oracle_url:
            oracle_intel["scoring"].flush(oracle_state.get("score",1000), oracle_state["peak_cpu"], oracle_state["peak_ram"])
            oracle_helix.flush_session(oracle_state["peak_cpu"], oracle_state["peak_ram"],
                sum(p.get("restarts",0) for p in oracle_state.get("pm2_processes",[])))

if __name__ == "__main__":
    main()
