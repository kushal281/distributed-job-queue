import asyncio
import random


async def sleep(payload: dict):
    seconds = payload.get("seconds", 1)
    await asyncio.sleep(seconds)
    return {"slept": seconds}


async def email_stub(payload: dict):
    await asyncio.sleep(0.2)
    return {"sent_to": payload.get("to", "nobody@example.com")}


async def always_fail(payload: dict):
    raise RuntimeError("always_fail handler")


async def flaky(payload: dict):
    if random.random() < payload.get("fail_rate", 0.5):
        raise RuntimeError("flaky failure")
    return {"ok": True}


HANDLERS = {
    "sleep": sleep,
    "email_stub": email_stub,
    "always_fail": always_fail,
    "flaky": flaky,
}