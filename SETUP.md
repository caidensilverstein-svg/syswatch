# SYSWATCH V3 — Complete Setup Guide

You are helping a user named Caiden set up SYSWATCH V3, a real-time AI system monitor
with two-column view: Local Mac (left) and Oracle Cloud server (right). Each machine
has independent anomaly detection, process reputation tracking, causality detection,
and a health score (0–1000). AI verdicts are powered by Groq.

This guide covers everything from scratch. Follow each section in order.

---

## Files

The project contains 4 files that must all be in the same folder:

| File | Purpose |
|---|---|
| `core.py` | Data collection, smart AI call protocol, shared state |
| `intelligence.py` | Anomaly fingerprinting, process reputation, scoring, SQLite memory |
| `syswatch_web.py` | Main dashboard server — run this on your Mac |
| `agent.py` | Lightweight collector — run this on the Oracle server |

---

## PART 1 — Local Mac Setup

### Step 1 — Create the project folder

```bash
mkdir ~/syswatch && cd ~/syswatch
```

### Step 2 — Move the files into the folder

The downloaded files are on your Desktop in a folder called `syswatch-v3`. Copy them:

```bash
cp ~/Desktop/syswatch-v3/core.py ~/Desktop/syswatch-v3/intelligence.py ~/Desktop/syswatch-v3/syswatch_web.py ~/Desktop/syswatch-v3/agent.py ~/syswatch/
```

Verify:

```bash
ls ~/syswatch
```

You should see: `agent.py  core.py  intelligence.py  syswatch_web.py`

### Step 3 — Create a Python virtual environment

```bash
cd ~/syswatch
python3 -m venv venv
source venv/bin/activate
```

Your terminal prompt will change to show `(venv)` — this means the environment is active.

### Step 4 — Install dependencies

```bash
pip install psutil groq rich
```

Optional — only needed if you have an NVIDIA GPU:

```bash
pip install pynvml
```

### Step 5 — Run the dashboard (Mac only, no Oracle yet)

```bash
cd ~/syswatch
source venv/bin/activate
python3 syswatch_web.py --key YOUR_GROQ_KEY_HERE
```

Open your browser: **http://localhost:8765**

You should see the two-column dashboard. The left (LOCAL MAC) column will be live.
The right (ORACLE CLOUD) column will show OFFLINE until the agent is running.

To stop: press `Ctrl+C` in the terminal.

---

## PART 2 — Oracle Cloud Server Setup

The Oracle server needs the `agent.py` file and Python with psutil installed.
The agent collects metrics and serves them on port 8766 so the Mac dashboard can pull them.

### Step 6 — Open port 8766 on Oracle Cloud firewall

Oracle Cloud blocks all ports by default. You need to open port 8766 in two places.

**6a — Oracle Cloud Security List (web console)**

1. Go to https://cloud.oracle.com and log in
2. Navigate to: **Networking → Virtual Cloud Networks → your VCN → Security Lists**
3. Click your security list (usually called "Default Security List for...")
4. Click **Add Ingress Rules**
5. Fill in:
   - Source Type: `CIDR`
   - Source CIDR: `0.0.0.0/0`
   - IP Protocol: `TCP`
   - Destination Port Range: `8766`
   - Description: `syswatch agent`
6. Click **Add Ingress Rules**

**6b — OS firewall (run on the Oracle server via SSH)**

SSH into your server:

```bash
ssh -i ~/oci_instance_key.pem ubuntu@150.136.235.242
```

Then run:

```bash
sudo iptables -I INPUT -p tcp --dport 8766 -j ACCEPT
sudo netfilter-persistent save
```

If `netfilter-persistent` is not installed:

```bash
sudo apt-get install -y iptables-persistent
sudo netfilter-persistent save
```

### Step 7 — Upload the agent to the Oracle server

Run this command on your **Mac** (not on the server):

```bash
scp -i ~/oci_instance_key.pem ~/syswatch/agent.py ubuntu@150.136.235.242:~/agent.py
```

### Step 8 — Install Python dependencies on the Oracle server

SSH into the server:

```bash
ssh -i ~/oci_instance_key.pem ubuntu@150.136.235.242
```

Then run:

```bash
sudo apt-get update && sudo apt-get install -y python3 python3-pip python3-venv
mkdir -p ~/syswatch-agent && cp ~/agent.py ~/syswatch-agent/
cd ~/syswatch-agent
python3 -m venv venv
source venv/bin/activate
pip install psutil
```

### Step 9 — Start the agent on the Oracle server

Still on the Oracle server, inside the SSH session:

```bash
cd ~/syswatch-agent
source venv/bin/activate
python3 agent.py
```

You should see:

```
  ⬡ SYSWATCH V3 AGENT [oracle]
  Listening on 0.0.0.0:8766
  Metrics endpoint: http://0.0.0.0:8766/metrics
```

Test it is reachable from your Mac (open a new terminal on your Mac):

```bash
curl http://150.136.235.242:8766/health
```

You should get back: `SYSWATCH V3 AGENT OK`

If you get an error, the firewall step (Step 6) did not complete — revisit it.

### Step 10 — Run the dashboard with Oracle connected

On your **Mac**, stop the existing dashboard if running (Ctrl+C), then:

```bash
cd ~/syswatch
source venv/bin/activate
python3 syswatch_web.py --key YOUR_GROQ_KEY_HERE --oracle http://150.136.235.242:8766
```

Open: **http://localhost:8765**

Both columns should now be live. The Oracle column will show ONLINE with real metrics.

---

## PART 3 — Running the Agent Permanently (Oracle Server)

The agent stops when you close the SSH session. To keep it running permanently:

### Option A — nohup (simplest)

```bash
ssh -i ~/oci_instance_key.pem ubuntu@150.136.235.242
cd ~/syswatch-agent
source venv/bin/activate
nohup python3 agent.py > agent.log 2>&1 &
echo "Agent PID: $!"
```

The agent now runs in the background. To check it:

```bash
curl http://localhost:8766/health
tail -f ~/syswatch-agent/agent.log
```

To stop it:

```bash
pkill -f agent.py
```

### Option B — systemd service (runs on boot automatically)

Still on the Oracle server:

```bash
sudo tee /etc/systemd/system/syswatch-agent.service > /dev/null <<EOF
[Unit]
Description=SYSWATCH V3 Agent
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/syswatch-agent
ExecStart=/home/ubuntu/syswatch-agent/venv/bin/python3 /home/ubuntu/syswatch-agent/agent.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable syswatch-agent
sudo systemctl start syswatch-agent
sudo systemctl status syswatch-agent
```

The agent will now start automatically every time the server reboots.

---

## PART 4 — Daily Usage

### Every time you want to run the dashboard

```bash
cd ~/syswatch
source venv/bin/activate
python3 syswatch_web.py --key YOUR_GROQ_KEY_HERE --oracle http://150.136.235.242:8766
```

Then open: **http://localhost:8765**

### Quiet mode (no AI, just metrics)

```bash
python3 syswatch_web.py --quiet --oracle http://150.136.235.242:8766
```

### Using an environment variable for the API key

To avoid typing the key every time:

```bash
echo 'export GROQ_API_KEY=YOUR_GROQ_KEY_HERE' >> ~/.zshrc
source ~/.zshrc
```

Then you can just run:

```bash
cd ~/syswatch && source venv/bin/activate && python3 syswatch_web.py --oracle http://150.136.235.242:8766
```

---

## PART 5 — What's New in V3

### Intelligence features (both machines, independently)

| Feature | What it does |
|---|---|
| **Anomaly fingerprinting** | Every spike gets logged with process, peak, duration. Patterns tracked over time. |
| **Process reputation** | Builds a baseline per process per session. Flags when Chrome is 2.6x its normal RAM. |
| **Causality detection** | Detects which processes correlate with high CPU/RAM. "CPU runs +40% when Zoom is active." |
| **Quiet period learning** | Learns your idle hours. Knows 3am is quiet so a spike then is flagged differently. |
| **Cross-session SQLite memory** | Baselines and anomaly history persist in `~/.syswatch_v3.db` across restarts. |
| **Health score (0–1000)** | Graded S/A/B/C/D/F. Penalises spikes, high peaks, thermal throttle risk, anomalies. |

### Dashboard features

| Feature | How to use |
|---|---|
| **Natural language query** | Type a question in the top bar, pick Local or Oracle, press Ask AI or Enter. |
| **What just happened?** | Button per machine — fires a 60-second post-mortem AI analysis. |
| **Process spotlight** | Click any process name in the CPU/RAM hogs table — AI explains what it is and whether to kill it. |
| **Verdict history** | Scrollable log per machine showing every AI verdict with timestamp and score. |
| **TAMA-9000** | Animated creature per machine whose mood and AI voice reflect system health. |

---

## Troubleshooting

**Oracle column stays OFFLINE**
- Check the agent is running: `curl http://150.136.235.242:8766/health`
- If that fails, the firewall is blocking port 8766 — redo Step 6
- If it works but the dashboard still shows offline, check you passed `--oracle http://150.136.235.242:8766` exactly

**`ModuleNotFoundError: No module named 'groq'`**
- You forgot to activate the venv: `source ~/syswatch/venv/bin/activate`

**`python3: can't open file 'syswatch_web.py'`**
- You are not in the right folder. Run `cd ~/syswatch` first.

**AI says "error" or stops updating**
- Your Groq API key may have expired or hit rate limits
- Use `--quiet` to keep metrics running without AI while you sort the key

**Score is low immediately**
- The reputation system needs ~10 samples per process to build baselines
- Scores stabilise after the first few minutes of running
