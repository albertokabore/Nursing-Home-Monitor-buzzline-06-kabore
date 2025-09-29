# Nursing Home Fall Risk Monitor (MFS) — Real-Time Streaming

## By Albert Kabore

A real-time monitoring pipeline for a 30-resident nursing home that computes Morse Fall Scale (MFS) risk, raises alerts, and orchestrates hourly rounding for nurses & CNAs.
Events stream over Kafka. The consumer renders one live Matplotlib figure:

Left: Top-N residents by current risk (color-coded bars) with the raw MFS total /125 on each bar.

Right: Time Watch (local time, current 8-hour shift, countdown to next rounding slot), Rounding assignments for the current slot, and Recent alerts.

⚠️ For education only — not a medical device and not for clinical use or decision-making.

Quick start (Windows / PowerShell)
# 1) Clone & open
git clone https://github.com/albertokabore/Nursing-Home-Monitor-buzzline-06-kabore.git
cd Nursing-Home-Monitor-buzzline-06-kabore

# 2) Create & activate virtual env
py -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3) Install deps
pip install -r requirements.txt

# 4) Configure environment
copy .env.example .env   # then edit as needed

# 5) Start Kafka (in separate terminals outside this repo)
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties

# 6) Create topic (first time only)
.\bin\windows\kafka-topics.bat --create --topic fall_risk_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 7) Run producer (in repo, with venv active)
py -m producers.event_producer

# 8) Run consumer (new terminal, same venv)
py -m consumers.event_consumer


You’ll see:

Console throughput (e.g., consumer: 120 msgs/s | residents: 30) + ALERT HIGH / ALERT WATCH lines.

One live figure updating in real time.

Repository layout
.
# ├─ producers/

# │  ├─ event_producer.py      # streams synthetic resident events (JSON)
# │  ├─ generators.py          # synthetic data + MFS helpers
# │  ├─ roster.py              # 30 residents + unit assignments
# │  └─ config.py              # reads .env for producer
# ├─ consumers/
# │  ├─ event_consumer.py      # real-time UI (single figure)
# │  ├─ mfs.py                 # MFS scoring & buckets
# │  ├─ risk.py                # fallback risk (partial features)
# │  ├─ rounding.py            # hourly rounding engine (nurse & CNA)
# │  └─ config.py              # reads .env for consumer
# ├─ utils/
# ├─ data/                     # (optional) seeds / logs
# ─ requirements.txt
# ├─ .env.example
# └─ README.md

# Requirements

Python 3.10+

Apache Kafka locally (ZooKeeper + Broker)

VS Code (recommended)

macOS / Linux notes

Activate the venv with source .venv/bin/activate and use the bin/* shell scripts instead of Windows .bat scripts.

Configuration (.env)

Copy .env.example → .env and adjust:

# Kafka
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=fall_risk_events

# Visualization
TOP_N=6                 # how many residents to display in the bar chart

# Rounding (slot length in seconds; use 3600 for hourly, lower for demos)
ROUNDING_PERIOD_SEC=3600
# Which staff schedules are active (defined in consumers/rounding.py)
ROUNDING_ACTIVE=nurse_u1,nurse_u2,nurse_u3,cna_u1,cna_u2,cna_u3

Data model (Kafka JSON event)

Each message is a single resident snapshot:

```json
{
  "resident_id": "R021",
  "resident_name": "Barbara Hernandez",
  "ts_utc": "2025-09-29T21:57:03Z",

  "mfs_history_fall": true,
  "mfs_secondary_dx": true,
  "mfs_ambulatory_aid": "crutches/cane/walker",   // none|bedrest|wheelchair | crutches/cane/walker | furniture
  "mfs_iv_heparin": false,
  "mfs_gait": "impaired",                         // normal|bedrest|wheelchair | weak | impaired
  "mfs_mental_status": "forgets_limitations",     // or oriented

  "mfs_points": {                                  // optional (producer may include)
    "history_fall": 25,
    "secondary_dx": 15,
    "ambulatory_aid": 15,
    "iv_heparin": 0,
    "gait": 20,
    "mental_status": 15
  },
  "mfs_total": 90,                                 // optional (consumer re-checks)

  "recent_near_fall": true,
  "gait_speed_mps": 0.45,
  "step_time_cv": 0.26,
  "sit_to_stand_time_s": 5.2,

  "bed_alarm_on": false,
  "call_bell_within_reach": false,
  "floor_wet": true,
  "clutter": false,
  "footwear_safe": false,
  "orthostatic_drop_sys": true,

  "unit": "U2",
  "room": "210B"
}
```

## Morse Fall Scale (MFS) points

Item	Options → Points

History of falling	Yes → 25, No → 0

Secondary diagnosis	Yes → 15, No → 0

Ambulatory aid	none/bedrest/wheelchair → 0; crutches/cane/walker → 15; furniture → 30

IV / Heparin lock	Yes → 20, No → 0

Gait/Transferring	normal/bedrest/wheelchair → 0; weak → 10; impaired → 20

Mental status	forgets limitations / overestimates ability → 15; oriented → 0

Buckets: LOW < 25, MODERATE 25–44, HIGH ≥ 45.

Normalized risk: MFS / 125 (shown on bars).

The consumer prefers full MFS when present; if partial, it computes a conservative risk from available drivers (see consumers/risk.py).

What the consumer shows

Top-N risk bars (green/orange/red) with the raw MFS/125 overlay.

Time Watch: Local time, current 8-hour shift (Day 07–15, Evening 15–23, Night 23–07), and countdown to the next rounding slot.

Rounding: For the latest slot, shows unit + role + staff + residents.

Recent alerts: ALERT HIGH (≥45) and ALERT WATCH (25–44) with concise drivers, e.g.:

ALERT HIGH — Paul King (0.64) — history_fall/secondary_dx/gait=weak

### Customization

Right panel width: edit PANEL_LEFT in consumers/event_consumer.py
(smaller value = wider panel; e.g., 0.58).

How many bars: .env → TOP_N

Rounding interval: .env → ROUNDING_PERIOD_SEC (e.g., 120 for demos).

Who rounds: .env → ROUNDING_ACTIVE (keys defined in rounding.py).

Colors / thresholds: color_for_score() and buckets in mfs.py.

Troubleshooting

Figure opens but no updates: Make sure producer is running and Kafka is reachable. Watch consumer console throughput.

Kafka smoke test:

.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic fall_risk_events --from-beginning --max-messages 3


Matplotlib “Animation was deleted” warning: already handled by assigning anim and calling plt.show(). If you still see it, ensure only one Python instance owns the figure.

Backend issues: We force TkAgg with fallback to Qt5Agg. Install Tk/Qt if needed.

CC6.2 / CC6.3 checklist (for your submission)

✅ Root files: requirements.txt, .gitignore, .env (+ example), folders for producers, consumers, utils, data

✅ New repo initialized and pushed to GitHub

✅ Kafka: ZooKeeper + Broker started, topic created, producer sending

✅ Consumer: running, reading live, figure animating, alerts & rounding visible

✅ Screenshot: VS Code with Explorer, active .venv, terminals, and the live figure

License & privacy

MIT-style educational use.

Uses synthetic data. Do not stream real PHI or patient data.