import time
from typing import Optional

import dramatiq
import pytest
from dramatiq.middleware import threading

from dramatiq_abort import Abort, Abortable, EventBackend, abort
from dramatiq_abort.middleware import AbortMode

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

    # And an actor that handles shutdown interrupts
    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        try:
            for _ in range(100):
                time.sleep(0.1)
        except Abort:
            aborts.append(1)
            raise
        successes.append(1)

    stub_broker.emit_after("process_boot")

    # If I send it a message
    message = do_work.send()

    # Then wait and signal the task to terminate
    time.sleep(1)
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

    # And an actor that handles shutdown interrupts
    @dramatiq.actor(abortable=True, max_retries=0)
    def do_work() -> None:
        try:
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
    time.sleep(0.1)
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

    @dramatiq.actor(abortable=False)
    def not_abortable() -> None:
        try:
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
    time.sleep(0.1)
    abort(message.message_id)

    # Then join on the queue
    stub_broker.join(not_abortable.queue_name)
    stub_worker.join()

    # I expect it to shutdown
    assert sum(aborts) == 0
    assert sum(successes) == 1


def test_abort_before_processing(
    stub_broker: dramatiq.Broker,
    stub_event_backend: EventBackend,
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
    # And abort right after.
    abort(
        message.message_id,
    )

    # Then start the worker.
    worker = dramatiq.Worker(stub_broker, worker_timeout=100, worker_threads=1)
    worker.start()

    stub_broker.join(do_work.queue_name)
    worker.join()
    worker.stop()

    # If I send it a message
    message = do_work.send()
    # And cancel right after.
    abort(message.message_id, mode=AbortMode.CANCEL)

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
