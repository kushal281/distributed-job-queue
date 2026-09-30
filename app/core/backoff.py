import random

BASE_S = 2
MAX_S = 60


def backoff_ms(attempts: int) -> int:
    """attempts = how many times the job has run so far (>= 1)."""
    delay = min(BASE_S * 2 ** (attempts - 1), MAX_S)
    jitter = random.uniform(0, BASE_S)
    return int((delay + jitter) * 1000)