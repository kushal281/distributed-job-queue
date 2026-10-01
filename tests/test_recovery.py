import uuid

import pytest
import pytest_asyncio

from app.core.db import SessionLocal
from app.core.redis_client import redis_client
from app.models.tables import Job, JobAttempt
from app.queue.queue import DELAYED, PROCESSING, READY
from app.worker.recovery import recover_once

pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_redis():
    await redis_client.delete(READY, DELAYED, PROCESSING)
    yield
    await redis_client.delete(READY, DELAYED, PROCESSING)


async def make_job(**kw) -> str:
    async with SessionLocal() as s:
        job = Job(type="sleep", payload={"seconds": 1}, **kw)
        s.add(job)
        await s.commit()
        return str(job.id)


async def load(job_id: str) -> Job:
    async with SessionLocal() as s:
        return await s.get(Job, uuid.UUID(job_id))


async def test_queued_job_missing_from_redis_is_reenqueued():
    job_id = await make_job(status="queued", priority=3)   # Postgres only, like a lost enqueue
    await recover_once()
    assert await redis_client.zscore(READY, job_id) is not None


async def test_job_already_in_redis_is_left_alone():
    job_id = await make_job(status="queued", priority=3)
    await redis_client.zadd(READY, {job_id: 123})
    await recover_once()
    assert await redis_client.zscore(READY, job_id) == 123   # not re-scored, not duplicated


async def test_running_job_without_lease_is_reclaimed():
    job_id = await make_job(status="running", attempts=1, max_attempts=3)
    async with SessionLocal() as s:
        s.add(JobAttempt(job_id=uuid.UUID(job_id), worker_id="dead-worker"))
        await s.commit()
    await recover_once()
    job = await load(job_id)
    assert job.status == "delayed"
    assert await redis_client.zscore(DELAYED, job_id) is not None


async def test_running_job_with_lease_is_untouched():
    job_id = await make_job(status="running", attempts=1, max_attempts=3)
    await redis_client.zadd(PROCESSING, {job_id: 9_999_999_999_999})
    await recover_once()
    assert (await load(job_id)).status == "running"