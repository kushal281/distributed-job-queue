from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from sqlalchemy import text

from app.api.routes_jobs import router as jobs_router, dlq_router
from app.api.routes_stats import stats_router
from app.core.db import engine
from app.core.redis_client import redis_client
from app.models.tables import Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Job Queue", lifespan=lifespan)
app.include_router(jobs_router)
app.include_router(dlq_router)
app.include_router(stats_router)

@app.get("/health")
async def health(response: Response):
    checks = {}
    try:
        checks["redis"] = "ok" if await redis_client.ping() else "fail"
    except Exception as e:
        checks["redis"] = f"fail: {e}"
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"fail: {e}"

    healthy = all(v == "ok" for v in checks.values())
    if not healthy:
        response.status_code = 503
    return {"status": "ok" if healthy else "degraded", **checks}