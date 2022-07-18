import logging
import time
from threading import Event
from typing import Optional
from unittest import mock

import dramatiq
import pytest
from _pytest.logging import LogCaptureFixture
from dramatiq.middleware import threading

from dramatiq_abort import Abort, Abortable, EventBackend, abort, abort_requested
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
    abortable.before_worker_boot(stub_broker, stub_worker)
    test_event = Event()

    # And an actor that handles shutdown interrupts
    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        try:
            test_event.set()
            for _ in range(11):  # Total has to be greater than abortable.wait_timeout
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

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
    abortable.before_worker_boot(stub_broker, stub_worker)
    test_event = Event()

    # And an actor that handles shutdown interrupts
    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        try:
            test_event.set()
            for _ in range(11):  # Total has to be greater than abortable.wait_timeout
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

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


@pytest.mark.skipif(not_supported, reason="Threading not supported on this platform.")
def test_abort_with_timeout(
    stub_broker: dramatiq.Broker,
    stub_worker: dramatiq.Worker,
    event_backend: EventBackend,
) -> None:
    # Given that I have a database
    aborts, successes, cleanups = [], [], []

    abortable = Abortable(backend=event_backend)
    stub_broker.add_middleware(abortable)
    abortable.before_worker_boot(stub_broker, stub_worker)
    test_event = Event()

    # And an actor that checks for abort requests
    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        try:
            test_event.set()
            for _ in range(11):  # Total has to be greater than abortable.wait_timeout
                remaining_time = abort_requested()
                if remaining_time:
                    cleanups.append(remaining_time)
                    break
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

    # If I send it a message
    message = do_work.send()

    # Then wait and signal the task to terminate, allowing for a timeout
    test_event.wait()
    abort(message.message_id, abort_timeout=1000)

    # Then join on the queue
    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # It should be able to finish cleanly
    assert cleanups[0] < 1000
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
    abortable.before_worker_boot(stub_broker, stub_worker)
    test_event = Event()

    @dramatiq.actor(abortable=False)
    def not_abortable() -> None:
        try:
            test_event.set()
            for _ in range(11):  # Total has to be greater than abortable.wait_timeout
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

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
    stub_worker: dramatiq.Worker,
    stub_event_backend: EventBackend,
    mode: AbortMode,
) -> None:
    calls = []
    abortable = Abortable(backend=stub_event_backend)
    stub_broker.add_middleware(abortable)
    abortable.before_worker_boot(stub_broker, stub_worker)

    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        calls.append(1)

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
    abortable.before_worker_boot(stub_broker, stub_worker)

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
@mock.patch("dramatiq_abort.abort_manager.raise_thread_exception")
def test_worker_abort_messages(
    raise_thread_exception: mock.Mock,
    stub_event_backend: EventBackend,
    caplog: LogCaptureFixture,
) -> None:
    # capture all messages
    caplog.set_level(logging.NOTSET)

    # Create fake threads
    thread1 = mock.Mock()
    thread1.ident = 1
    thread2 = mock.Mock()
    thread2.ident = 2

    # Given a middleware with abortable "threads"
    middleware = Abortable(backend=stub_event_backend)
    middleware.manager.abortable_messages |= {  # type: ignore
        "fake_message_id_overdue": thread1,
        "fake_message_id_delayed": thread2,
    }

    # Add an overdue abort request and a delayed one
    middleware.manager.abort_requests[thread1] = (
        "fake_message_id_overdue",
        time.monotonic() - 10,
    )
    middleware.manager.abort_requests[thread2] = (
        "fake_message_id_delayed",
        time.monotonic() + 10,
    )
    middleware.manager.abort_pending()

    # An abort exception is raised only in the overdue thread
    raise_thread_exception.assert_has_calls([mock.call(1, Abort)])

    # And abort actions are logged
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples == [
        (
            "dramatiq_abort.middleware.Abortable",
            logging.INFO,
            (f"Aborting task. Raising exception in worker thread {thread1!r}."),
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

    # Create greenlets
    greenlet1 = gevent.spawn()
    greenlet2 = gevent.spawn()

    # Given a middleware with abortable "threads"
    middleware = Abortable(backend=stub_event_backend)
    middleware.manager.abortable_messages |= {  # type: ignore
        "fake_message_id_overdue": greenlet1,
        "fake_message_id_delayed": greenlet2,
    }

    # Add an overdue abort request and a delayed one
    middleware.manager.abort_requests[greenlet1] = (
        "fake_message_id_overdue",
        time.monotonic() - 10,
    )
    middleware.manager.abort_requests[greenlet2] = (
        "fake_message_id_delayed",
        time.monotonic() + 10,
    )
    middleware.manager.abort_pending()

    # An abort exception is raised only in the overdue thread
    assert isinstance(greenlet1.exception, Abort)
    assert greenlet2.exception is None

    # And abort actions are logged
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples == [
        (
            "dramatiq_abort.middleware.Abortable",
            logging.INFO,
            (f"Aborting task. Raising exception in worker thread {greenlet1!r}."),
        )
    ]
