import asyncio
import uuid

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select

from app.api.main import app
from app.core.db import SessionLocal
from app.core.redis_client import redis_client
from app.models.tables import Job
from app.queue.queue import READY

pytestmark = pytest.mark.asyncio(loop_scope="session")

BODY = {"type": "sleep", "payload": {"seconds": 1}}


@pytest_asyncio.fixture(loop_scope="session")
async def client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def new_key() -> str:
    return f"test-{uuid.uuid4()}"


def post(client, key=None, body=BODY):
    headers = {"Idempotency-Key": key} if key else {}
    return client.post("/jobs", json=body, headers=headers)


async def count_jobs(key: str) -> int:
    async with SessionLocal() as s:
        res = await s.execute(select(func.count()).select_from(Job).where(Job.idempotency_key == key))
        return res.scalar_one()


async def test_duplicate_returns_original_job(client):
    key = new_key()
    a = await post(client, key)
    b = await post(client, key)
    assert a.status_code == 201
    assert b.status_code == 200
    assert a.json()["id"] == b.json()["id"]
    assert await count_jobs(key) == 1
    # enqueued (workers must be stopped, like the other tests)
    assert await redis_client.zscore(READY, a.json()["id"]) is not None


async def test_same_key_different_body_is_rejected(client):
    key = new_key()
    assert (await post(client, key)).status_code == 201
    r = await post(client, key, {"type": "sleep", "payload": {"seconds": 99}})
    assert r.status_code == 422
    assert await count_jobs(key) == 1


async def test_concurrent_duplicates_create_one_job(client):
    key = new_key()
    results = await asyncio.gather(*[post(client, key) for _ in range(10)])
    assert len({r.json()["id"] for r in results}) == 1
    codes = sorted(r.status_code for r in results)
    assert codes == [200] * 9 + [201]
    assert await count_jobs(key) == 1


async def test_postgres_unique_index_backs_up_redis(client):
    key = new_key()
    a = await post(client, key)
    await redis_client.delete(f"idem:{key}")     # simulate Redis losing the key
    b = await post(client, key)
    assert b.status_code == 200
    assert a.json()["id"] == b.json()["id"]
    assert await count_jobs(key) == 1


async def test_no_key_means_no_dedup(client):
    a = await post(client)
    b = await post(client)
    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]