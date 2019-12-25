from dramatiq_abort.backends import RedisBackend


def test_redis_backend_wait_many_error(redis_event_backend: RedisBackend) -> None:
    # A key without the sentinel value returns None.
    redis_event_backend.client.rpush(b"test", b"not-x")
    assert redis_event_backend.wait_many([b"test"], timeout=1000) is None
