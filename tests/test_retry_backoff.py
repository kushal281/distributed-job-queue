from app.core.backoff import backoff_ms


def test_backoff_grows_and_caps():
    assert 2000 <= backoff_ms(1) <= 4000
    assert 4000 <= backoff_ms(2) <= 6000
    assert 8000 <= backoff_ms(3) <= 10000
    assert backoff_ms(20) <= 62000       # capped at 60s + up to 2s jitter