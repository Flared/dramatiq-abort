import threading
import time
import warnings
from enum import Enum
from logging import Logger
from threading import Thread
from typing import Any, Dict, List, Optional, Set, Tuple

import dramatiq
from dramatiq import get_broker
from dramatiq.logging import get_logger
from dramatiq.middleware import Middleware, SkipMessage
from dramatiq.middleware.threading import (
    Interrupt,
    current_platform,
    raise_thread_exception,
    supported_platforms,
)

from .backend import EventBackend


def is_gevent_active() -> bool:
    """Detect if gevent monkey patching is active.

    This function should be removed and replaced with the identically named
    function imported from dramatiq.middleware.threading as soon as a
    dependency on a version of dramatiq with this function can be made.
    """
    try:
        from gevent import monkey
    except ImportError:  # pragma: no cover
        return False
    return bool(monkey.saved)


class Abort(Interrupt):
    """Exception used to interrupt worker threads when their worker
    processes have been signaled to abort.
    """


class AbortMode(Enum):
    """
    Abort in following mode.

    In "cancel" mode, only pending message will be aborted,
    running message will also be aborted additionally in "abort" mode.
    """

    ABORT = "abort"
    CANCEL = "cancel"


class Abortable(Middleware):
    """Middleware that interrupts actors whose job has been signaled for
    termination.
    Currently, this is only available on CPython.

    This middleware also adds an ``abortable`` option that can be set on
    dramatiq ``actor`` and ``send_with_options``. Value priority is respectively
    ``send_with_options``, ``actor`` and this ``Abortable``.

    Note: This works by setting an async exception in the worker thread
    that runs the actor.  This means that the exception will only get
    called the next time that thread acquires the GIL. Concretely,
    this means that this middleware can't cancel system calls.

    :param backend: Event backend used to signal termination from a broker to
        the workers. See :class:`RedisBackend`.
    :type backend: :class:`EventBackend`

    :param abortable: Set the default value for every actor ``abortable``
        option.
    """

    def __init__(
        self,
        *,
        backend: EventBackend,
        abortable: bool = True,
        abort_ttl: int = 90_000,
    ):
        self.logger = get_logger(__name__, type(self))
        self.abortable = abortable
        self.backend = backend
        self.wait_timeout = 1000
        self.abort_ttl = abort_ttl
        self.manager: Any = None
        if is_gevent_active():
            self.manager = _GeventAbortManager(self.logger)
        else:
            self.manager = _CtypesAbortManager(self.logger)

    @property
    def actor_options(self) -> Set[str]:
        return {"abortable"}

    def is_abortable(self, actor: dramatiq.Actor, message: dramatiq.Message) -> bool:
        abortable = message.options.get("abortable")
        if abortable is None:
            abortable = actor.options.get("abortable")
        if abortable is None:
            abortable = self.abortable
        return bool(abortable)

    def before_worker_boot(
        self, broker: dramatiq.Broker, worker: dramatiq.Worker
    ) -> None:
        if current_platform in supported_platforms:
            thread = Thread(target=self._watcher, daemon=True)
            thread.start()
        else:  # pragma: no cover
            msg = "Abortable cannot kill threads on your current platform (%r)."
            warnings.warn(msg % current_platform, category=RuntimeWarning, stacklevel=2)

    def before_process_message(
        self, broker: dramatiq.Broker, message: dramatiq.Message
    ) -> None:
        actor = broker.get_actor(message.actor_name)
        if not self.is_abortable(actor, message):
            return

        args = self.backend.poll(self.id_to_key(message.message_id, AbortMode.CANCEL))
        if args is not None:
            raise SkipMessage()

        self.manager.add_abortable(message.message_id)

    def after_process_message(
        self,
        broker: dramatiq.Broker,
        message: dramatiq.Message,
        *,
        result: Optional[Any] = None,
        exception: Optional[BaseException] = None
    ) -> None:
        self.manager.remove_abortable(message.message_id)

    after_skip_message = after_process_message

    def abort(
        self,
        message_id: str,
        abort_ttl: Optional[int] = None,
        mode: AbortMode = AbortMode.ABORT,
        abort_timeout: int = 0,
    ) -> None:
        if abort_ttl is None:
            abort_ttl = self.abort_ttl
        modes: List[Tuple[AbortMode, Dict[str, Any]]]
        modes = [(AbortMode.CANCEL, dict())]
        if mode is AbortMode.ABORT:
            modes.append((AbortMode.ABORT, dict(abort_timeout=abort_timeout)))
        values = [(self.id_to_key(message_id, mode), args) for mode, args in modes]
        self.backend.notify(values, ttl=abort_ttl)

    def check_abort_request(self) -> Optional[float]:
        abort_time: Optional[float]
        _, abort_time = self.manager.abort_requests.get(
            threading.get_ident(), (None, None)
        )
        if abort_time is None:
            return None
        return 1000 * (abort_time - time.monotonic())

    def _get_abort_requests(self) -> None:
        message_ids = self.manager.abortable_messages.keys()
        if not message_ids:
            time.sleep(self.wait_timeout / 1000)
            return

        abort_keys = [self.id_to_key(id_, AbortMode.ABORT) for id_ in message_ids]
        key, args = self.backend.wait_many(abort_keys, self.wait_timeout)
        if not key:
            return

        message_id = self.key_to_id(key)
        self.manager.add_abort_request(message_id, **args)

    def _watcher(self) -> None:
        while True:
            try:
                self._get_abort_requests()
                self.manager.abort_pending()
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Unhandled error while running the time limit handler."
                )

    @staticmethod
    def id_to_key(message_id: str, mode: AbortMode = AbortMode.ABORT) -> str:
        return mode.value + ":" + message_id

    @staticmethod
    def key_to_id(key: str) -> str:
        return key.split(":", 1)[-1]


def _get_abortable_from_broker() -> Abortable:
    broker = get_broker()
    for middleware in broker.middleware:
        if isinstance(middleware, Abortable):
            return middleware
    else:
        raise RuntimeError("The default broker doesn't have an abortable backend.")


def abort(
    message_id: str,
    middleware: Optional[Abortable] = None,
    abort_ttl: Optional[int] = None,
    mode: AbortMode = AbortMode.ABORT,
    abort_timeout: int = 0,
) -> None:
    """Abort a pending or running message given its ``message_id``.

    :param message_id: Message to abort. Use the return value of ``actor.send``
        or ``actor.send_with_options`` to then use its ``.message_id`` attribute.

    :param middleware: :class:`Abortable` middleware used by the workers and
        broker used to signal termination. If set to ``None``, use the default broker
        from ``dramatiq.get_broker()`` and retrieve the configured :class:`Abortable`
        middleware. If no :class:`Abortable` middleware is set on the broker and
        ``middleware`` is ``None``, raises a :class:`RuntimeError`.
    :type middleware: :class:`Abortable`

    :param abort_ttl: Change default abort TTL value, optional argument. If set to
        ``None`` default value from :class:`Abortable` is used.

    :param mode: "AbortMode.ABORT" or "AbortMode.CANCEL".In "cancel" mode,
        only pending message will be aborted,
        running message will also be aborted additionally in "abort" mode.
    """
    if not middleware:
        middleware = _get_abortable_from_broker()

    middleware.abort(message_id, abort_ttl, mode, abort_timeout)


def abort_requested(middleware: Optional[Abortable] = None) -> Optional[float]:
    if not middleware:
        middleware = _get_abortable_from_broker()
    return middleware.check_abort_request()


class _CtypesAbortManager:
    """Manager for raising Abort exceptions via the ctypes api.

    :param logger: The logger for abort log lines.
    :type logger: :class:`logging.Logger`
    """

    def __init__(self, logger: Optional[Logger] = None):
        self.logger = logger or get_logger(__name__, type(self))
        # This lock avoid race between the monitor and a task cleaning up.
        self.lock = threading.Lock()
        self.abortable_messages: Dict[str, int] = {}
        self.abort_requests: Dict[int, Tuple[str, float]] = {}

    def add_abortable(self, message_id: str) -> None:
        self.abortable_messages[message_id] = threading.get_ident()

    def remove_abortable(self, message_id: str) -> None:
        with self.lock:
            thread_id = threading.get_ident()
            saved_thread_id = self.abortable_messages.pop(message_id, None)
            saved_message_id, _ = self.abort_requests.pop(thread_id, (None, None))
            assert not saved_thread_id or thread_id == saved_thread_id
            assert not saved_message_id or saved_message_id == message_id

    def add_abort_request(self, message_id: str, abort_timeout: int = 0) -> None:
        with self.lock:
            thread_id = self.abortable_messages.get(message_id, None)
            if thread_id is None:
                # If the task finished before we signaled it to abort
                return
            self.abort_requests[thread_id] = (
                message_id,
                time.monotonic() + abort_timeout / 1000,
            )

    def abort_pending(self) -> None:
        with self.lock:
            toabort = [
                thread_id
                for thread_id, (_, abort_time) in self.abort_requests.items()
                if time.monotonic() >= abort_time
            ]

            for thread_id in toabort:
                message_id, abort_time = self.abort_requests.pop(thread_id, ("", None))
                saved_thread_id = self.abortable_messages.pop(message_id, None)
                assert saved_thread_id == thread_id
                if thread_id is None or abort_time is None:
                    # If the task finished before abort_timeout passed
                    assert thread_id is None and abort_time is None
                    return

                self.logger.info(
                    "Aborting task. Raising exception in worker thread %r.", thread_id
                )
                raise_thread_exception(thread_id, Abort)


if is_gevent_active():
    from gevent import Greenlet, getcurrent

    class _GeventAbortManager:
        """Manager for raising Abort exceptions in green threads.

        :param logger: The logger for abort log lines.
        :type logger: :class:`logging.Logger`
        """

        def __init__(self, logger: Optional[Logger] = None):
            self.logger = logger or get_logger(__name__, type(self))
            self.abortables: Dict[str, Tuple[int, Greenlet]] = {}

        def add_abortable(self, message_id: str) -> None:
            self.abortables[message_id] = (threading.get_ident(), getcurrent())

        def remove_abortable(self, message_id: str) -> None:
            self.abortables.pop(message_id, (None, None))

        def abort(self, message_id: str) -> None:
            thread_id, greenlet = self.abortables.pop(message_id, (None, None))
            # In case the task was done in between the polling and now.
            if greenlet is None:
                return  # pragma: no cover

            self.logger.info(
                "Aborting task. Raising exception in worker thread %r.", thread_id
            )
            greenlet.kill(Abort, block=False)
