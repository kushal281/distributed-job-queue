from datetime import datetime, timezone

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.main import app
from app.core.db import SessionLocal
from app.core.redis_client import redis_client
from app.models.tables import Job
from app.queue.queue import READY, PROCESSING


@pytest.fixture(autouse=True)
async def clean_queue():
    await redis_client.delete(READY, PROCESSING)
    yield
    await redis_client.delete(READY, PROCESSING)


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def make_job(status: str):
    async with SessionLocal() as s:
        job = Job(type="sleep", status=status, attempts=3, max_attempts=3,
                  last_error="boom", priority=2, finished_at=datetime.now(timezone.utc))
        s.add(job)
        await s.commit()
        return job.id


async def test_dead_job_listed_then_retried(client):
    jid = await make_job("dead")

    assert str(jid) in [j["id"] for j in (await client.get("/dlq")).json()]

    r = await client.post(f"/jobs/{jid}/retry")
    assert r.status_code == 200
    assert r.json()["status"] == "queued" and r.json()["attempts"] == 0
    assert await redis_client.zscore(READY, str(jid)) is not None

    assert str(jid) not in [j["id"] for j in (await client.get("/dlq")).json()]


async def test_cannot_retry_non_dead_job(client):
    jid = await make_job("succeeded")
    r = await client.post(f"/jobs/{jid}/retry")
    assert r.status_code == 409
    assert await redis_client.zscore(READY, str(jid)) is None


async def test_retry_unknown_job_404(client):
    r = await client.post("/jobs/00000000-0000-0000-0000-000000000000/retry")
    assert r.status_code == 404