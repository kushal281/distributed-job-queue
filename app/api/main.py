from fastapi import FastAPI, Response
from sqlalchemy import text

from app.core.db import engine
from app.core.redis_client import redis_client

app = FastAPI(title="Job Queue")


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