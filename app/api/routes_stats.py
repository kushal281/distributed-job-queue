import time
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.core.redis_client import redis_client
from app.models.tables import Job
from app.queue.queue import DELAYED, PROCESSING, READY

stats_router = APIRouter(tags=["stats"])

WORKERS_SEEN = "workers:seen"      # ZSET: worker_id -> last heartbeat ms
FORGET_AFTER_MS = 600_000          # drop dead workers from the list after 10 min


@stats_router.get("/stats")
async def stats(session: AsyncSession = Depends(get_session)):
    rows = await session.execute(select(Job.status, func.count()).group_by(Job.status))
    jobs = {status: n for status, n in rows.all()}
    return {
        "queue": {
            "ready": await redis_client.zcard(READY),
            "delayed": await redis_client.zcard(DELAYED),
            "processing": await redis_client.zcard(PROCESSING),
        },
        "jobs": jobs,
        "dlq": jobs.get("dead", 0),
    }


@stats_router.get("/workers")
async def workers():
    now_ms = int(time.time() * 1000)
    await redis_client.zremrangebyscore(WORKERS_SEEN, "-inf", now_ms - FORGET_AFTER_MS)
    seen = await redis_client.zrange(WORKERS_SEEN, 0, -1, withscores=True)
    out = []
    for wid, ts in seen:
        val = await redis_client.get(f"worker:{wid}")   # TTL key: gone = heartbeat stopped
        out.append({
            "id": wid,
            "status": "alive" if val is not None else "dead",
            "current_jobs": int(val) if val else 0,
            "last_heartbeat_ms_ago": now_ms - int(ts),
        })
    return sorted(out, key=lambda w: w["id"])


@stats_router.get("/dashboard", include_in_schema=False)
async def dashboard():
    return FileResponse(Path(__file__).resolve().parents[1] / "dashboard" / "index.html")