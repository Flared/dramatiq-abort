import threading
import time
import warnings
from logging import Logger
from threading import Thread
from typing import Any, Dict, Optional, Set, Tuple

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
        the workers. See :any:`RedisBackend`.
    :type backend: :class:`EventBackend`

    :param abortable: Set the default value for every actor ``abortable``
        option.
    """

    def __init__(self, *, backend: EventBackend, abortable: bool = True):
        self.logger = get_logger(__name__, type(self))
        self.abortable = abortable
        self.backend = backend
        self.wait_timeout = 1000
        self.abort_ttl = 90000
        self.manager: Any = None
        if is_gevent_active():
            self.manager = _GeventAbortManager(self.logger)
        else:
            self.manager = _CtypesAbortManager(self.logger)
        # This lock avoid race between the monitor and a task cleaning up.
        self.lock = threading.Lock()

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

    def after_process_boot(self, broker: dramatiq.Broker) -> None:
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

        if self.backend.poll(self.id_to_key(message.message_id)):
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
        with self.lock:
            self.manager.remove_abortable(message.message_id)

    after_skip_message = after_process_message

    def abort(self, message_id: str, abort_ttl: Optional[int] = None) -> None:
        if abort_ttl is None:
            abort_ttl = self.abort_ttl

        self.backend.notify(self.id_to_key(message_id), ttl=abort_ttl)

    def _handle(self) -> None:
        message_ids = list(self.manager.abortables.keys())
        if not message_ids:
            time.sleep(self.wait_timeout / 1000)
            return

        abort_keys = [self.id_to_key(id_) for id_ in message_ids]
        key = self.backend.wait_many(abort_keys, self.wait_timeout)
        if not key:
            return

        message_id = self.key_to_id(key)
        with self.lock:
            self.manager.abort(message_id)

    def _watcher(self) -> None:
        while True:
            try:
                self._handle()
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Unhandled error while running the time limit handler."
                )

    @staticmethod
    def id_to_key(message_id: str) -> bytes:
        return ("abort:" + message_id).encode()

    @staticmethod
    def key_to_id(key: bytes) -> str:
        return key.decode()[6:]


def abort(
    message_id: str,
    middleware: Optional[Abortable] = None,
    abort_ttl: Optional[int] = None,
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
    """
    if not middleware:
        broker = get_broker()
        for middleware in broker.middleware:
            if isinstance(middleware, Abortable):
                break
        else:
            raise RuntimeError("The default broker doesn't have an abortable backend.")

    middleware.abort(message_id, abort_ttl)


class _CtypesAbortManager:
    """Manager for raising Abort exceptions via the ctypes api.

    :param logger: The logger for abort log lines.
    :type logger: :class:`logging.Logger`
    """

    def __init__(self, logger: Optional[Logger] = None):
        self.logger = logger or get_logger(__name__, type(self))
        self.abortables: Dict[str, int] = {}

    def add_abortable(self, message_id: str) -> None:
        self.abortables[message_id] = threading.get_ident()

    def remove_abortable(self, message_id: str) -> None:
        self.abortables.pop(message_id, None)

    def abort(self, message_id: str) -> None:
        thread_id = self.abortables.pop(message_id, None)
        # In case the task was done in between the polling and now.
        if thread_id is None:
            return  # pragma: no cover

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
