#!/usr/bin/env python3
"""
SYSWATCH V4 — Oracle Agent with PM2/Cypher Infrastructure Monitoring
Exposes: http://0.0.0.0:8766/metrics

Usage: cd ~/syswatch-agent && source venv/bin/activate && python3 agent.py [--port 8766]
Requires: pip install psutil
"""

import os, sys, json, time, threading, subprocess, psutil
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from collections import deque
import argparse

HISTORY_LEN = 300
CRITICAL_PROCS = {"cloudflared", "cypher-proxy"}

state = {
    "machine":        "oracle",
    "cpu_pct":        0.0,
    "cpu_per_core":   [],
    "cpu_top":        [],
    "cpu_history":    [],
    "ram_pct":        0.0,
    "ram_used_gb":    0.0,
    "ram_total_gb":   0.0,
    "ram_top":        [],
    "ram_history":    [],
    "cpu_temps":      [],
    "cpu_temp_max":   0.0,
    "thermal_throttle_risk": False,
    "has_battery":    False,
    "battery_pct":    None,
    "battery_charging": None,
    "battery_mins_left": None,
    "gpus":           [],
    "gpu_available":  False,
    "net_connections": [],
    "net_io":         None,
    "peak_cpu":       0.0,
    "peak_ram":       0.0,
    "session_start":  time.time(),
    "uptime_s":       0,
    "ts":             0,
    "pm2_processes":  [],
    "pm2_available":  False,
    "cypher_online":  False,
    "critical_down":  [],
    "total_restarts": 0,
    "pm2_alerts":     [],
    "pm2_last_updated": "—",
}

_cpu_hist  = deque(maxlen=HISTORY_LEN)
_ram_hist  = deque(maxlen=HISTORY_LEN)
_prev_restarts = {}

def collect_pm2():
    try:
        result = subprocess.run(
            ["pm2", "jlist"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            state["pm2_available"] = False
            return

        raw = json.loads(result.stdout)
        processes = []
        alerts    = []
        total_restarts = 0
        critical_down  = []

        for p in raw:
            name      = p.get("name", "unknown")
            pm2_env   = p.get("pm2_env", {})
            status    = pm2_env.get("status", "unknown")
            restarts  = pm2_env.get("restart_time", 0)
            uptime_ms = pm2_env.get("pm_uptime", 0)
            pid       = p.get("pid", None)
            cpu       = p.get("monit", {}).get("cpu", 0)
            mem_bytes = p.get("monit", {}).get("memory", 0)
            mem_mb    = round(mem_bytes / 1e6, 1)
            version   = pm2_env.get("version", "N/A")

            uptime_s = 0
            if uptime_ms and status == "online":
                uptime_s = int((time.time() * 1000 - uptime_ms) / 1000)

            total_restarts += restarts
            prev = _prev_restarts.get(name, restarts)
            new_crashes = max(0, restarts - prev)
            _prev_restarts[name] = restarts

            if status != "online":
                alerts.append(f"{name} is {status.upper()}")
                if name in CRITICAL_PROCS:
                    critical_down.append(name)
            elif new_crashes > 0:
                alerts.append(f"{name} crashed {new_crashes}x just now (total: {restarts})")
            elif restarts > 50 and name == "cypher-proxy":
                alerts.append(f"cypher-proxy: {restarts} lifetime restarts — instability detected")

            processes.append({
                "id":          p.get("pm_id", 0),
                "name":        name,
                "status":      status,
                "pid":         pid,
                "cpu":         cpu,
                "mem_mb":      mem_mb,
                "restarts":    restarts,
                "uptime_s":    uptime_s,
                "version":     version,
                "is_critical": name in CRITICAL_PROCS,
                "new_crashes": new_crashes,
            })

        processes.sort(key=lambda x: (not x["is_critical"], x["name"]))

        state["pm2_processes"]    = processes
        state["pm2_available"]    = True
        state["total_restarts"]   = total_restarts
        state["critical_down"]    = critical_down
        state["pm2_alerts"]       = alerts
        state["cypher_online"]    = all(
            p["status"] == "online" for p in processes
            if p["name"] in CRITICAL_PROCS
        ) and len(processes) > 0
        state["pm2_last_updated"] = datetime.now().strftime("%H:%M:%S")

    except Exception as e:
        state["pm2_available"] = False
        state["pm2_alerts"]    = [f"pm2 error: {e}"]

def collect():
    state["cpu_pct"]      = psutil.cpu_percent(interval=0.5)
    state["cpu_per_core"] = psutil.cpu_percent(percpu=True)
    state["peak_cpu"]     = max(state["peak_cpu"], state["cpu_pct"])

    vm = psutil.virtual_memory()
    state["ram_pct"]      = vm.percent
    state["ram_used_gb"]  = round(vm.used  / 1e9, 2)
    state["ram_total_gb"] = round(vm.total / 1e9, 2)
    state["peak_ram"]     = max(state["peak_ram"], state["ram_pct"])

    _cpu_hist.append(round(state["cpu_pct"], 1))
    _ram_hist.append(round(state["ram_pct"], 1))
    state["cpu_history"] = list(_cpu_hist)
    state["ram_history"] = list(_ram_hist)

    procs = []
    for p in psutil.process_iter(["name","cpu_percent","memory_info"]):
        try: procs.append(p.info)
        except: pass

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

    temps = []
    try:
        raw = psutil.sensors_temperatures()
        for chip, entries in (raw or {}).items():
            for e in entries:
                if e.current and e.current > 0:
                    temps.append((e.label or chip, round(e.current, 1)))
    except: pass
    state["cpu_temps"]    = temps[:8]
    state["cpu_temp_max"] = max((t for _,t in temps), default=0.0)
    state["thermal_throttle_risk"] = state["cpu_temp_max"] >= 85

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
    except: pass
    state["net_connections"] = conns[:15]

    try:
        nio = psutil.net_io_counters()
        state["net_io"] = {"bytes_sent": nio.bytes_sent, "bytes_recv": nio.bytes_recv}
    except:
        state["net_io"] = None

    state["uptime_s"] = int(time.time() - psutil.boot_time())
    state["ts"]       = time.time()

def collect_loop():
    tick = 0
    while True:
        try:
            collect()
            if tick % 3 == 0:
                collect_pm2()
            tick += 1
        except: pass
        time.sleep(1)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def log_message(self, *a): pass

    def send_json(self, obj, status=200):
        data = json.dumps(obj, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        import subprocess
        from urllib.parse import urlparse, parse_qs
        p  = urlparse(self.path)
        qs = parse_qs(p.query)

        if p.path in ("/metrics", "/metrics/"):
            self.send_json(state)

        elif p.path in ("/health", "/"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            status = "ONLINE" if state["cypher_online"] else "DEGRADED"
            self.wfile.write(f"SYSWATCH V5 AGENT OK — CYPHER {status}".encode())

        elif p.path == "/exec":
            cmd = qs.get("cmd", ["echo ok"])[0]
            # Whitelist safe commands only
            safe_prefixes = ("pm2", "ps", "free", "df", "top", "ls", "cat",
                             "uptime", "ss", "netstat", "curl", "ping",
                             "systemctl status", "journalctl", "echo")
            is_safe = any(cmd.strip().startswith(s) for s in safe_prefixes)
            if not is_safe:
                self.send_json({"output": "", "error": "Command not in safe list"}, 403)
                return
            try:
                result = subprocess.run(cmd, shell=True, capture_output=True,
                                        text=True, timeout=10)
                self.send_json({"output": result.stdout + result.stderr})
            except Exception as e:
                self.send_json({"output": "", "error": str(e)})

        else:
            self.send_response(404)
            self.end_headers()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port",    default=8766, type=int)
    parser.add_argument("--machine", default="oracle")
    parser.add_argument("--host",    default="0.0.0.0")
    args = parser.parse_args()

    state["machine"] = args.machine
    collect_pm2()

    print(f"\n  ⬡ SYSWATCH V4 AGENT [{args.machine}]")
    print(f"  Listening: {args.host}:{args.port}")
    print(f"  PM2: {'available — ' + str(len(state['pm2_processes'])) + ' processes' if state['pm2_available'] else 'NOT FOUND'}")
    for p in state["pm2_processes"]:
        icon = "✓" if p["status"] == "online" else "✕"
        print(f"    {icon} {p['name']:<22} restarts: {p['restarts']}")
    print(f"  Cypher: {'ONLINE' if state['cypher_online'] else 'DEGRADED'}\n")

    threading.Thread(target=collect_loop, daemon=True).start()
    server = HTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped.\n")

if __name__ == "__main__":
    main()
