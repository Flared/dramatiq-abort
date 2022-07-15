from dramatiq_abort import Event
from dramatiq_abort.backends import RedisBackend


def test_redis_namespace(redis_event_backend: RedisBackend) -> None:
    redis_event_backend.namespace = "test_namespace:"
    event = Event(key="test_key", params=dict())
    redis_event_backend.notify([event], 1000)
    assert redis_event_backend.client.lindex("test_namespace:test_key", 0) == b"{}"
    assert redis_event_backend.wait_many([event.key], timeout=1000) == event


def test_redis_decoding(redis_event_backend: RedisBackend) -> None:
    redis_event_backend.namespace = "test_namespace:"
    valid_event = Event(key="test_key3", params=dict(valid=1))
    redis_event_backend.client.rpush("test_namespace:test_key1", b"not-a-json")
    redis_event_backend.client.rpush("test_namespace:test_key2", b'["not-a-dict"]')
    redis_event_backend.client.rpush("test_namespace:test_key3", b'{"valid": 1}')
    assert redis_event_backend.wait_many(["test_key1"], timeout=1000) is None
    assert redis_event_backend.poll("test_key2") is None
    assert redis_event_backend.poll("test_key3") == valid_event
