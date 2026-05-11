#!/usr/bin/env python3
"""
SYSWATCH V2 — Terminal Edition
Usage: python3 syswatch.py [--key YOUR_GROQ_KEY] [--quiet]
Requires: pip install psutil groq rich
Optional:  pip install pynvml  (NVIDIA GPU support)
"""

import os, sys, time, threading, argparse
from datetime import datetime
from groq import Groq
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.live import Live
from rich import box
from rich.align import Align
from rich.columns import Columns

sys.path.insert(0, os.path.dirname(__file__))
import core

# ── CONFIG ────────────────────────────────────────────────────────────────────
MODEL = "llama-3.1-8b-instant"

console = Console()
client  = None
quiet   = False

# ── AI ENGINE ─────────────────────────────────────────────────────────────────
def _call_groq(prompt: str, max_tokens: int = 200, temp: float = 0.85) -> str:
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        temperature=temp,
    )
    return resp.choices[0].message.content.strip()

def fetch_verdicts():
    core.state["ai_status"] = "fetching"
    try:
        text = _call_groq(core.build_verdict_prompt(), max_tokens=220)
        cpu_v = ram_v = tama = ""
        for line in text.splitlines():
            if line.startswith("CPU VERDICT:"):
                cpu_v = line.replace("CPU VERDICT:", "").strip()
            elif line.startswith("RAM VERDICT:"):
                ram_v = line.replace("RAM VERDICT:", "").strip()
            elif line.startswith("TAMAGOTCHI:"):
                tama  = line.replace("TAMAGOTCHI:", "").strip()
        if cpu_v: core.state["cpu_verdict"] = cpu_v
        if ram_v: core.state["ram_verdict"] = ram_v
        if tama:  core.state["tamagotchi_msg"] = tama
        combined = f"{cpu_v} | {ram_v}"
        core.push_verdict_history(combined)
        core.state["last_updated"] = datetime.now().strftime("%H:%M:%S")
        core.state["ai_status"]    = "ready"
        core.state["ai_backoff"]   = 15  # reset backoff on success
        core.record_ai_called()
    except Exception as e:
        core.state["cpu_verdict"] = f"AI error: {e}"
        core.state["ai_status"]   = "error"
        # Exponential backoff
        core.state["ai_backoff"]  = min(core.MAX_AI_BACKOFF, core.state["ai_backoff"] * 2)

def maybe_fetch_digest():
    now = time.time()
    if now - core.state["last_digest_ts"] >= 300:  # every 5 min
        try:
            core.state["digest"]        = _call_groq(core.build_digest_prompt(), max_tokens=120, temp=0.7)
            core.state["last_digest_ts"] = now
        except Exception:
            pass

def maybe_fetch_prediction():
    try:
        core.state["prediction"] = _call_groq(core.build_prediction_prompt(), max_tokens=80, temp=0.7)
    except Exception:
        pass

def ai_loop():
    while True:
        if not quiet:
            should, reason = core.should_call_ai()
            if should:
                fetch_verdicts()
                # Piggyback prediction every other verdict
                if core.state["ai_call_count"] % 2 == 0:
                    maybe_fetch_prediction()
            maybe_fetch_digest()
        time.sleep(2)

# ── TAMAGOTCHI ASCII ──────────────────────────────────────────────────────────
TAMA_FRAMES = {
    "happy":     ["(=^.^=)", "( ^ω^ )", "(≧◡≦)"],
    "content":   ["(－‿－)", "( ˘ω˘ )", "(￣ω￣)"],
    "concerned": ["(°ロ°)", "(⊙_⊙)", "(ó﹏ò)"],
    "sweating":  ["(x_x;)", "(>_<;)", "(；一_一)"],
    "screaming": ["(╯°□°）╯", "ヽ(｀Д´)ﾉ", "(ﾉಠ益ಠ)ﾉ"],
}
_tama_frame_idx = 0

def get_tama_frame() -> str:
    global _tama_frame_idx
    mood   = core.state["tamagotchi_mood"]
    frames = TAMA_FRAMES.get(mood, TAMA_FRAMES["happy"])
    frame  = frames[_tama_frame_idx % len(frames)]
    _tama_frame_idx += 1
    return frame

# ── RENDER HELPERS ─────────────────────────────────────────────────────────────
TIER_COLORS = {
    "chill":    "bright_green",
    "normal":   "green",
    "elevated": "yellow",
    "hot":      "color(208)",
    "critical": "red1",
}
TIER_LABELS = {
    "chill":    "1/5 CHILL",
    "normal":   "2/5 NORMAL",
    "elevated": "3/5 ELEVATED",
    "hot":      "4/5 HOT",
    "critical": "5/5 CRITICAL",
}

def danger_bar(pct: float, width: int = 26) -> Text:
    tier   = core.tier_for(pct)
    color  = TIER_COLORS[tier]
    filled = int(pct / 100 * width)
    bar    = Text()
    bar.append("█" * filled,          style=color)
    bar.append("░" * (width - filled), style="color(238)")
    bar.append(f"  {pct:.1f}%",       style="bold " + color)
    return bar

def make_gauge_panel(label: str, pct: float, verdict: str, tier: str) -> Panel:
    color = TIER_COLORS[tier]
    t = Text()
    t.append(f"\n  {TIER_LABELS[tier]}\n\n", style=f"bold {color}")
    t.append("  ")
    t.append(danger_bar(pct))
    t.append(f"\n\n  {verdict}\n", style="italic color(252)")
    return Panel(t, title=f"[bold white]{label}[/]", border_style=color, padding=(0,1))

def make_core_panel() -> Panel:
    cores = core.state["cpu_per_core"]
    t = Text()
    t.append("\n")
    for i, pct in enumerate(cores):
        tier  = core.tier_for(pct)
        color = TIER_COLORS[tier]
        bar_w = 12
        filled = int(pct / 100 * bar_w)
        t.append(f"  C{i:<2} ", style="color(245)")
        t.append("█" * filled,              style=color)
        t.append("░" * (bar_w - filled),    style="color(238)")
        t.append(f" {pct:5.1f}%\n",         style=color)
    return Panel(t, title="[bold white]PER-CORE CPU[/]", border_style="color(238)", padding=(0,0))

def make_thermal_panel() -> Panel:
    temps = core.state["cpu_temps"]
    t = Text()
    t.append("\n")
    if not temps:
        t.append("  No sensor data available\n", style="color(245)")
    else:
        for label, temp in temps[:6]:
            if temp >= 85:   color = "red1"
            elif temp >= 70: color = "color(208)"
            elif temp >= 55: color = "yellow"
            else:            color = "bright_green"
            t.append(f"  {label[:14]:<14} ", style="color(245)")
            t.append(f"{temp:.0f}°C\n",       style=f"bold {color}")
        if core.state["thermal_throttle_risk"]:
            t.append("\n  ⚠ THROTTLE RISK\n", style="bold red1")
    return Panel(t, title="[bold white]THERMALS[/]", border_style="color(238)", padding=(0,0))

def make_battery_panel() -> Panel:
    t = Text()
    t.append("\n")
    if not core.state["has_battery"]:
        t.append("  No battery (desktop)\n", style="color(245)")
    else:
        pct     = core.state["battery_pct"] or 0
        charging = core.state["battery_charging"]
        mins    = core.state["battery_mins_left"]
        if pct < 20:   color = "red1"
        elif pct < 40: color = "color(208)"
        else:          color = "bright_green"
        icon = "⚡" if charging else "🔋"
        t.append(f"  {icon} {pct:.0f}%\n", style=f"bold {color}")
        if charging:
            t.append("  Charging\n", style="green")
        elif mins:
            t.append(f"  ~{mins} min remaining\n", style=color)
    return Panel(t, title="[bold white]BATTERY[/]", border_style="color(238)", padding=(0,0))

def make_gpu_panel() -> Panel:
    t = Text()
    t.append("\n")
    if not core.state["gpu_available"] or not core.state["gpus"]:
        t.append("  No GPU detected\n", style="color(245)")
        t.append("  (install pynvml for NVIDIA)\n", style="color(238)")
    else:
        for g in core.state["gpus"]:
            util_color = TIER_COLORS[core.tier_for(g["util_pct"])]
            t.append(f"  {g['name'][:22]}\n", style="bold white")
            t.append(f"  Util  {g['util_pct']:5.1f}%\n",       style=util_color)
            vram_pct = g["vram_used_mb"] / max(1, g["vram_total_mb"]) * 100
            vram_color = TIER_COLORS[core.tier_for(vram_pct)]
            t.append(f"  VRAM  {g['vram_used_mb']:5.0f}/{g['vram_total_mb']:.0f}MB\n", style=vram_color)
            t.append(f"  Temp  {g['temp_c']:5.0f}°C\n",        style=("red1" if g["temp_c"] >= 85 else "color(245)"))
            t.append(f"  Clock {g['clock_mhz']:5.0f}MHz\n",    style="color(245)")
    return Panel(t, title="[bold white]GPU[/]", border_style="color(238)", padding=(0,0))

def make_network_panel() -> Panel:
    conns = core.state["net_connections"]
    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold color(249)", padding=(0,1))
    tbl.add_column("PROCESS",  style="white",      no_wrap=True, width=16)
    tbl.add_column("REMOTE",   style="cyan",        no_wrap=True, width=22)
    tbl.add_column("STATUS",   style="color(245)",  no_wrap=True, width=12)
    for c in conns[:6]:
        tbl.add_row(c["proc"][:16], c["raddr"][:22], c["status"])
    if not conns:
        tbl.add_row("no connections", "", "")
    return Panel(tbl, title="[bold white]NETWORK CONNECTIONS[/]", border_style="color(238)")

def make_proc_table(title: str, rows: list, col: str) -> Panel:
    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold color(249)", padding=(0,1))
    tbl.add_column("PROCESS", style="white", no_wrap=True, width=20)
    tbl.add_column(col,       style="cyan",  justify="right", width=10)
    for name, val in rows:
        unit = "%" if col == "CPU %" else "MB"
        tbl.add_row(name[:20], f"{val:.1f}{unit}")
    return Panel(tbl, title=f"[bold color(249)]{title}[/]", border_style="color(238)")

def make_tama_panel() -> Panel:
    mood  = core.state["tamagotchi_mood"]
    frame = get_tama_frame()
    msg   = core.state.get("tamagotchi_msg", "…")
    mood_colors = {
        "happy":     "bright_green",
        "content":   "green",
        "concerned": "yellow",
        "sweating":  "color(208)",
        "screaming": "red1",
    }
    color = mood_colors.get(mood, "white")
    t = Text()
    t.append(f"\n  {frame}\n\n", style=f"bold {color}")
    t.append(f"  {mood.upper()}\n\n", style=f"dim {color}")
    t.append(f"  \"{msg[:55]}\"\n",  style=f"italic color(252)")
    return Panel(t, title="[bold white]TAMA-9000[/]", border_style=color, padding=(0,0))

def make_digest_panel() -> Panel:
    digest = core.state.get("digest", "Digest generates every 5 minutes…")
    pred   = core.state.get("prediction", "")
    t = Text()
    t.append(f"\n  {digest or 'Gathering data…'}\n", style="color(252)")
    if pred:
        t.append(f"\n  ⟶ {pred}\n", style="italic yellow")
    return Panel(t, title="[bold white]DIGEST + PREDICTION[/]", border_style="color(238)", padding=(0,1))

def make_header() -> Text:
    ai_tag = {
        "waiting":  "[color(245)]◌ AI STANDBY[/]",
        "fetching": "[yellow]⟳ FETCHING…[/]",
        "ready":    f"[green]✓ UPDATED {core.state['last_updated']}[/]",
        "error":    "[red1]✕ AI ERROR[/]",
    }.get(core.state["ai_status"], "")

    elapsed = int(time.time() - core.state["session_start"])
    h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
    t = Text()
    t.append("  ⬡ SYSWATCH V2", style="bold white")
    t.append("  —  ", style="color(245)")
    t.append_text(Text.from_markup(ai_tag))
    t.append(f"  │  calls: {core.state['ai_call_count']}  │  peak CPU {core.state['peak_cpu']:.0f}%  RAM {core.state['peak_ram']:.0f}%", style="color(242)")
    t.append(f"  │  uptime {h:02d}:{m:02d}:{s:02d}", style="color(242)")
    if quiet:
        t.append("  [QUIET MODE]", style="dim yellow")
    return t

def render() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header",  size=3),
        Layout(name="gauges",  size=11),
        Layout(name="row2",    size=10),
        Layout(name="row3",    size=10),
        Layout(name="digest",  size=6),
        Layout(name="footer",  size=1),
    )
    layout["header"].update(Panel(Align.center(make_header()), border_style="color(234)", padding=(0,0)))

    layout["gauges"].split_row(
        Layout(make_gauge_panel("CPU", core.state["cpu_pct"], core.state["cpu_verdict"], core.state["cpu_tier"])),
        Layout(make_gauge_panel("RAM", core.state["ram_pct"], core.state["ram_verdict"], core.state["ram_tier"])),
        Layout(make_tama_panel(), ratio=1),
    )

    layout["row2"].split_row(
        Layout(make_core_panel()),
        Layout(make_thermal_panel()),
        Layout(make_battery_panel()),
        Layout(make_gpu_panel()),
    )

    layout["row3"].split_row(
        Layout(make_proc_table("TOP CPU HOGS", core.state["cpu_top"], "CPU %")),
        Layout(make_proc_table("TOP RAM HOGS", core.state["ram_top"], "RAM MB")),
        Layout(make_network_panel()),
    )

    layout["digest"].update(make_digest_panel())
    layout["footer"].update(
        Align.center(Text(
            f"  q quit  │  data: 1s  │  AI: adaptive  │  quiet mode: {'ON' if quiet else 'OFF (--quiet to enable)'}  ",
            style="color(242)"
        ))
    )
    return layout

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    global client, quiet

    parser = argparse.ArgumentParser(description="SYSWATCH V2 Terminal Monitor")
    parser.add_argument("--key",   default=os.environ.get("GROQ_API_KEY",""), help="Groq API key")
    parser.add_argument("--model", default=MODEL, help="Groq model to use")
    parser.add_argument("--quiet", action="store_true", help="Suppress AI calls (metrics only)")
    args = parser.parse_args()

    quiet = args.quiet

    if not args.key and not quiet:
        print("Error: Groq API key required. Use --key or set GROQ_API_KEY env var.")
        print("       Use --quiet for metrics-only mode (no AI calls).")
        sys.exit(1)

    if args.key:
        client = Groq(api_key=args.key)

    console.clear()

    threading.Thread(target=core.data_loop, daemon=True).start()
    threading.Thread(target=ai_loop,        daemon=True).start()

    with Live(render(), refresh_per_second=1, screen=True) as live:
        try:
            while True:
                live.update(render())
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    console.print("\n[color(245)]SYSWATCH V2 terminated.[/]\n")

if __name__ == "__main__":
    main()
