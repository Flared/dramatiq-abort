from dramatiq_abort import Event, EventBackend


def test_backend(event_backend: EventBackend) -> None:
    timeout = 1000
    test_a = Event(key="test-a", params={})
    test_b = Event(key="test-b", params={"param1": 1})
    test_c = Event(
        key="test-c", params={"param1": 1, "param2": "string", "param3": True}
    )
    test_d = Event(key="test-d", params={})

    assert event_backend.wait_many([test_a.key], timeout=timeout) is None

    event_backend.notify([test_a], timeout)
    event_backend.notify([test_b, test_c], timeout)

    assert event_backend.wait_many([test_a.key, test_b.key], timeout=timeout) == test_a
    assert event_backend.wait_many([test_a.key, test_b.key], timeout=timeout) == test_b
    assert event_backend.poll(test_c.key) == test_c
    assert event_backend.poll(test_d.key) is None
    assert (
        event_backend.wait_many(
            [test_a.key, test_b.key, test_c.key, test_d.key], timeout=timeout
        )
        is None
    )
