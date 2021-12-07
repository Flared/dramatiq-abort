import logging
import time
from threading import Event
from typing import Optional
from unittest import mock

import dramatiq
import pytest
from _pytest.logging import LogCaptureFixture
from dramatiq.middleware import threading

from dramatiq_abort import Abort, Abortable, EventBackend, abort
from dramatiq_abort.middleware import AbortMode, is_gevent_active

not_supported = threading.current_platform not in threading.supported_platforms


@pytest.mark.skipif(not_supported, reason="Threading not supported on this platform.")
def test_abort_notifications_are_received(
    stub_broker: dramatiq.Broker,
    stub_worker: dramatiq.Worker,
    event_backend: EventBackend,
) -> None:
    # Given that I have a database
    aborts, successes = [], []

    abortable = Abortable(backend=event_backend)
    stub_broker.add_middleware(abortable)
    test_event = Event()

    # And an actor that handles shutdown interrupts
    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        try:
            test_event.set()
            for _ in range(10):
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

    stub_broker.emit_after("process_boot")

    # If I send it a message
    message = do_work.send()

    # Then wait and signal the task to terminate
    test_event.wait()
    abort(message.message_id)

    # Then join on the queue
    stub_broker.join(do_work.queue_name)
    stub_worker.join()
    assert aborts
    assert not successes


@pytest.mark.skipif(not_supported, reason="Threading not supported on this platform.")
def test_cancel_notifications_are_received(
    stub_broker: dramatiq.Broker,
    stub_worker: dramatiq.Worker,
    event_backend: EventBackend,
) -> None:
    # Given that I have a database
    aborts, successes = [], []

    abortable = Abortable(backend=event_backend)
    stub_broker.add_middleware(abortable)
    test_event = Event()

    # And an actor that handles shutdown interrupts
    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        try:
            test_event.set()
            for _ in range(10):
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

    stub_broker.emit_after("process_boot")

    # If I send it a message
    message = do_work.send()

    # Then wait
    test_event.wait()
    abort(message.message_id, mode=AbortMode.CANCEL)

    # Then join on the queue
    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # Task will finished, the cancel won't take any effect.
    assert successes
    assert not aborts


def test_not_abortable(
    stub_broker: dramatiq.Broker,
    stub_worker: dramatiq.Worker,
    stub_event_backend: EventBackend,
) -> None:
    aborts, successes = [], []
    abortable = Abortable(backend=stub_event_backend)
    stub_broker.add_middleware(abortable)
    test_event = Event()

    @dramatiq.actor(abortable=False)
    def not_abortable() -> None:
        try:
            test_event.set()
            for _ in range(10):
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

    stub_broker.emit_after("process_boot")

    # If I send it a message
    message = not_abortable.send()

    # Then wait and signal the task to terminate
    test_event.wait()
    abort(message.message_id)

    # Then join on the queue
    stub_broker.join(not_abortable.queue_name)
    stub_worker.join()

    # I expect it to shutdown
    assert sum(aborts) == 0
    assert sum(successes) == 1


@pytest.mark.parametrize(
    "mode",
    [
        (AbortMode.ABORT,),
        (AbortMode.CANCEL,),
    ],
)
def test_abort_before_processing(
    stub_broker: dramatiq.Broker,
    stub_event_backend: EventBackend,
    mode: AbortMode,
) -> None:
    calls = []
    abortable = Abortable(backend=stub_event_backend)
    stub_broker.add_middleware(abortable)

    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        calls.append(1)

    stub_broker.emit_after("process_boot")

    # If I send it a message
    message = do_work.send()
    # And cancel right after.
    abort(message.message_id, mode=mode)

    # Then start the worker.
    worker = dramatiq.Worker(stub_broker, worker_timeout=100, worker_threads=1)
    worker.start()

    stub_broker.join(do_work.queue_name)
    worker.join()
    worker.stop()

    # I expect the task to not have been called.
    assert sum(calls) == 0


@pytest.mark.parametrize(
    "middleware_abortable,actor_abortable,message_abortable,is_abortable",
    [
        (True, None, None, True),
        (True, False, None, False),
        (True, True, None, True),
        (True, None, False, False),
        (True, False, False, False),
        (True, True, False, False),
        (True, None, True, True),
        (True, False, True, True),
        (True, True, True, True),
        (False, None, None, False),
        (False, False, None, False),
        (False, True, None, True),
        (False, None, False, False),
        (False, False, False, False),
        (False, True, False, False),
        (False, None, True, True),
        (False, False, True, True),
        (False, True, True, True),
    ],
)
def test_abortable_configs(
    stub_event_backend: EventBackend,
    middleware_abortable: bool,
    actor_abortable: Optional[bool],
    message_abortable: Optional[bool],
    is_abortable: bool,
) -> None:
    abortable = Abortable(backend=stub_event_backend, abortable=middleware_abortable)

    message = dramatiq.Message(
        queue_name="some-queue",
        actor_name="some-actor",
        args=(),
        kwargs={},
        options={"abortable": message_abortable},
    )

    @dramatiq.actor(abortable=actor_abortable)
    def actor() -> None:
        pass

    assert abortable.is_abortable(actor, message) == is_abortable


def test_abort_polling(
    stub_broker: dramatiq.Broker,
    stub_worker: dramatiq.Worker,
    stub_event_backend: EventBackend,
) -> None:
    sentinel = []
    abortable = Abortable(backend=stub_event_backend)
    stub_broker.add_middleware(abortable)

    @dramatiq.actor(abortable=True, max_retries=0)
    def abort_with_delay() -> None:
        try:
            sentinel.append(True)
            time.sleep(5)
            abortable.abort(message.message_id)
            for _ in range(20):
                time.sleep(0.1)
            sentinel.append(True)
        except Abort:
            sentinel.append(False)
            raise
        sentinel.append(True)

    stub_broker.emit_after("process_boot")

    # If I send it a message
    message = abort_with_delay.send()

    # Then join on the queue
    stub_broker.join(abort_with_delay.queue_name)
    stub_worker.join()

    # I expect it to shutdown
    assert sentinel == [True, False]


def test_abort_with_no_middleware(
    stub_broker: dramatiq.Broker, stub_worker: dramatiq.Worker
) -> None:
    try:
        abort("foo")
        raise AssertionError("Exception not raised")
    except RuntimeError:
        assert True


@pytest.mark.skipif(is_gevent_active(), reason="Test behaviour is dependent on gevent.")
@mock.patch("dramatiq_abort.middleware.raise_thread_exception")
def test_worker_abort_messages(
    raise_thread_exception: mock.Mock,
    stub_event_backend: EventBackend,
    caplog: LogCaptureFixture,
) -> None:
    # capture all messages
    caplog.set_level(logging.NOTSET)

    # Given a middleware with an abortable "thread"
    middleware = Abortable(backend=stub_event_backend)
    middleware.manager.abortables = {"fake_message_id": 1}

    # When the message is aborted
    middleware.manager.abort("fake_message_id")

    # An abort exception is raised in the thread
    raise_thread_exception.assert_has_calls([mock.call(1, Abort)])

    # And abort actions are logged
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples == [
        (
            "dramatiq_abort.middleware.Abortable",
            logging.INFO,
            ("Aborting task. Raising exception in worker thread 1."),
        )
    ]


@pytest.mark.skipif(
    not is_gevent_active(), reason="Test behaviour is dependent on gevent."
)
def test_gevent_worker_abort_messages(
    stub_event_backend: EventBackend, caplog: LogCaptureFixture
) -> None:
    import gevent

    # capture all messages
    caplog.set_level(logging.NOTSET)

    # Given a middleware with an abortable "thread"
    greenlet = gevent.spawn()
    middleware = Abortable(backend=stub_event_backend)
    middleware.manager.abortables = {"fake_message_id": (1, greenlet)}

    # When the message is aborted
    middleware.manager.abort("fake_message_id")

    # An abort exception is raised in the thread
    assert isinstance(greenlet.exception, Abort)

    # And abort actions are logged
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples == [
        (
            "dramatiq_abort.middleware.Abortable",
            logging.INFO,
            ("Aborting task. Raising exception in worker thread 1."),
        )
    ]
