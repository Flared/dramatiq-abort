from dramatiq_abort import Event
from dramatiq_abort.backends import RedisBackend


def test_redis_namespace(redis_event_backend: RedisBackend) -> None:
    event = Event(key="test_key", params=dict())
    redis_event_backend.namespace = "test_namespace:"
    redis_event_backend.notify([event], 1000)
    assert redis_event_backend.client.lindex("test_namespace:test_key", 0) == b"{}"
    assert redis_event_backend.wait_many([event.key], timeout=1000) == event
