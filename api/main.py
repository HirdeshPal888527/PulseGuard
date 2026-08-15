import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_DB = os.environ.get("PG_DB", "anomaly_engine")
PG_USER = os.environ.get("PG_USER", "anomaly")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "anomaly")

app = FastAPI(title="Real-Time Anomaly Engine API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_pool: Optional[asyncpg.Pool] = None


class WindowMetric(BaseModel):
    window_start: datetime
    window_end: datetime
    user_id: str
    event_count: int
    mean_amount: float
    stddev_amount: Optional[float]
    max_amount: float


class Anomaly(BaseModel):
    event_time: datetime
    user_id: str
    amount: float
    z_score: float
    window_mean: float
    window_stddev: float
    location: Optional[str]
    device_id: Optional[str]


@app.on_event("startup")
async def startup():
    global _pool
    _pool = await asyncpg.create_pool(
        host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD,
        min_size=1, max_size=5,
    )


@app.on_event("shutdown")
async def shutdown():
    if _pool:
        await _pool.close()


@app.get("/health")
async def health():
    try:
        async with _pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/metrics/recent", response_model=list[WindowMetric])
async def recent_metrics(
    minutes: int = Query(15, ge=1, le=1440),
    user_id: Optional[str] = None,
    limit: int = Query(500, ge=1, le=5000),
):
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    sql = """
        SELECT window_start, window_end, user_id, event_count,
               mean_amount, stddev_amount, max_amount
        FROM window_metrics
        WHERE window_start >= $1
        {user_filter}
        ORDER BY window_start DESC
        LIMIT $2
    """
    params = [since, limit]
    user_filter = ""
    if user_id:
        user_filter = "AND user_id = $3"
        params.append(user_id)

    async with _pool.acquire() as conn:
        rows = await conn.fetch(sql.format(user_filter=user_filter), *params)
    return [dict(r) for r in rows]


@app.get("/anomalies/live", response_model=list[Anomaly])
async def live_anomalies(
    minutes: int = Query(15, ge=1, le=1440),
    limit: int = Query(200, ge=1, le=2000),
):
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    sql = """
        SELECT event_time, user_id, amount, z_score,
               window_mean, window_stddev, location, device_id
        FROM anomalies
        WHERE event_time >= $1
        ORDER BY event_time DESC
        LIMIT $2
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(sql, since, limit)
    return [dict(r) for r in rows]


@app.get("/anomalies/stats")
async def anomaly_stats(minutes: int = Query(60, ge=1, le=1440)):
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    sql = """
        SELECT count(*) AS total_anomalies,
               count(DISTINCT user_id) AS distinct_users,
               avg(z_score) AS avg_z_score,
               max(z_score) AS max_z_score
        FROM anomalies
        WHERE event_time >= $1
    """
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(sql, since)
    return dict(row)
