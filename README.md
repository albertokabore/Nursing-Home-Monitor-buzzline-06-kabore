# Nursing Home Fall Risk Monitor — Real‑time (Kafka + Morse Fall Scale) with Alerts & Hourly Rounding
- 30 residents, Kafka **producer** + **consumer**, official **Morse Fall Scale** (per‑item points, total 0–125, bucket).
- **Single real-time plot** (Top‑6 bars), colors by MFS bucket (green/orange/red), dashed average line, wrapped names.
- **Risk alerts** (terminal + right-side panel): `ALERT WATCH` (Moderate) and `ALERT HIGH` (High) with driver hints.
- **Hourly Rounding** (consumer-side): alternates **Nurse ↔ CNA** each period across all assigned residents during active shifts.
- Consumer seeks to the **live tail**, runs a background poll thread, and prints a `msgs/s` heartbeat.

## Quickstart (Windows PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env

# Start ZooKeeper + Kafka so localhost:9092 is reachable

# Terminal 1 – Producer
python -m producers.event_producer

# Terminal 2 – Consumer (plot + alerts + rounding)
python -m consumers.event_consumer
```
> For demo speed, set `ROUNDING_PERIOD_SEC=120` in `.env` (every 2 minutes).
