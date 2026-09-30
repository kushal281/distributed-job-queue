import asyncio
import uuid

import pytest

from app.core.redis_client import redis_client
from app.queue.queue import READY, PROCESSING, claim, enqueue


@pytest.fixture(autouse=True)
async def clean_queue():
    await redis_client.delete(READY, PROCESSING)
    yield
    await redis_client.delete(READY, PROCESSING)


async def test_priority_then_fifo():
    low, high1, high2 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await enqueue(low, 5)
    await asyncio.sleep(0.01)
    await enqueue(high1, 1)
    await asyncio.sleep(0.01)
    await enqueue(high2, 1)

    order = [await claim(), await claim(), await claim()]
    assert order == [str(high1), str(high2), str(low)]
    assert await claim() is None


async def test_concurrent_claims_never_duplicate():
    ids = {str(uuid.uuid4()) for _ in range(50)}
    for i in ids:
        await enqueue(i, 5)

    results = await asyncio.gather(*[claim() for _ in range(100)])
    claimed = [r for r in results if r is not None]

    assert len(claimed) == 50              # every job claimed
    assert len(set(claimed)) == 50         # none claimed twice
    assert set(claimed) == ids
    assert await redis_client.zcard(PROCESSING) == 50