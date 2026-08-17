# Real-Time Streaming Analytics & Anomaly Engine

A self-contained streaming data pipeline that ingests high-volume transaction events, computes per-user statistical baselines over sliding time windows, flags outliers with a dynamic Z-score, and serves the results through a REST API and a live dashboard — all running locally with a single `docker compose up`.

## Overview

Most transaction fraud/anomaly demos train a model on a static, already-labeled dataset. This project instead solves the problem the way it actually shows up in production: an unbounded stream of events, no fixed dataset, and a detector that has to keep its notion of "normal" continuously up to date per user, in real time, without a human labeling every row.

The core idea is a **per-user dynamic Z-score**. Instead of a single global threshold (e.g. "flag anything over $500"), each user's own rolling mean and standard deviation define what's normal *for them*. A $500 charge is unremarkable for a user who typically spends $800 and highly anomalous for one who typically spends $20.

## Architecture

```
producer.py  →  Redpanda (Kafka API)  →  Spark Structured Streaming  →  TimescaleDB  →  FastAPI  →  Streamlit
                                                │                              ▲
                                     5-min sliding window,                    │
                                     1-min watermark, per-user           hypertables +
                                     mean/stddev, Z-score scoring        retention policy
```

| Stage | Technology | What it does |
|---|---|---|
| Event generation | Python + `kafka-python` | Simulates ~500 events/sec across 200 users, each with a distinct log-normal spending profile; periodically injects labeled outlier transactions |
| Message broker | Redpanda (Kafka API-compatible) | Partitions events by `user_id`, decouples ingestion from processing, absorbs bursty load |
| Stream processing | PySpark Structured Streaming | 5-minute sliding window (10s slide), 1-minute watermark for late data, running mean/stddev per user, Z-score anomaly scoring |
| Storage | TimescaleDB (Postgres + hypertables) | Time-partitioned storage for windowed metrics and flagged anomalies, with a 7-day retention policy |
| API | FastAPI | Read layer exposing recent metrics, live anomalies, and summary stats |
| Dashboard | Streamlit + Plotly | Auto-refreshing charts of transaction volume/amount with anomalies overlaid, plus a live anomaly feed |

## Getting started

**Requirements:** Docker and Docker Compose, ~4GB free RAM.

```bash
git clone <this-repo>
cd realtime-anomaly-engine
docker compose up --build
```

Once the containers are healthy:

| Service | URL |
|---|---|
| Streamlit dashboard | http://localhost:8501 |
| FastAPI docs (Swagger) | http://localhost:8000/docs |
| Redpanda Console | http://localhost:8080 |
| TimescaleDB | `localhost:5432` (`anomaly` / `anomaly`) |

Stop and remove everything, including volumes:

```bash
docker compose down -v
```

## Project structure

```
.
├── docker-compose.yml
├── producer/            # synthetic event generator
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── spark/               # structured streaming job
│   ├── spark_processor.py
│   ├── Dockerfile
├── sql/
│   └── init.sql         # TimescaleDB schema, hypertables, retention policy
├── api/                 # FastAPI read layer
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/            # Streamlit UI
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
└── .github/workflows/ci.yml
```

## API reference

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness/readiness check |
| `GET /metrics/recent?minutes=15&user_id=&limit=500` | Recent windowed aggregates |
| `GET /anomalies/live?minutes=15&limit=200` | Recently flagged anomalies |
| `GET /anomalies/stats?minutes=60` | Summary counts and average/max Z-score over a window |

## How detection works

1. The producer emits transaction events keyed by `user_id`, drawn from a log-normal distribution so most transactions are small and a few are naturally large — a realistic spending pattern that defeats naive fixed thresholds.
2. Spark consumes the stream with a 5-minute sliding window (recomputed every 10 seconds) and a 1-minute watermark, computing `mean_amount` and `stddev_amount` per user per window.
3. Each incoming event is scored against its user's current window statistics:

   ```
   Z = (amount - mean_amount) / stddev_amount
   ```

4. Events with `|Z| > 3.0` are written to the `anomalies` table along with the window statistics that triggered the flag.
5. The producer also tags each event it intentionally injects as an outlier (`injected_anomaly`), which is never used by the detector itself but can be joined against `anomalies` offline to measure precision/recall and tune the Z-score threshold.

## Design notes

- **Sliding vs. tumbling windows** — a fixed-boundary window resets a user's "recent normal" every N minutes, which either misses fast-forming anomalies or over-reacts at window edges. A sliding window keeps the baseline continuously current at the cost of more compute.
- **Watermarking** — bounds how long Spark keeps window state open for late-arriving events, preventing unbounded memory growth while still tolerating some out-of-order delivery.
- **Per-user thresholding** — scoring against each user's own distribution, rather than a global dollar threshold, is what makes the detector meaningful across a population with very different spending patterns.

## Current limitations

- Anomaly scoring re-reads `window_metrics` from Postgres every micro-batch rather than keeping per-user state natively in Spark (`mapGroupsWithState`); simpler to run locally, but adds read load at scale.
- Runs as a single-node Spark job (`local[*]`); a production deployment would target a real cluster.
- The dashboard polls the API on an interval rather than receiving pushed updates over a WebSocket.

## License

MIT
