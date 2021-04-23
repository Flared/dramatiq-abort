from dramatiq_abort import EventBackend


def test_backend(event_backend: EventBackend) -> None:
    assert event_backend.wait_many([b"test"], timeout=1000) is None

    event_backend.notify([b"test-a"], 1000)
    event_backend.notify([b"test-b"], 1000)
    event_backend.notify([b"test-c"], 1000)

    assert event_backend.wait_many([b"test-a", b"test-b"], timeout=1000) == b"test-a"
    assert event_backend.wait_many([b"test-a", b"test-b"], timeout=1000) == b"test-b"
    assert event_backend.poll(b"test-c") is True
    assert event_backend.poll(b"test-d") is False
