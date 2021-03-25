import threading
import time
import warnings
from enum import Enum
from threading import Thread
from typing import Any, Dict, Optional, Set

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
        self.abortables: Dict[str, int] = {}
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

        if self.backend.poll(self.id_to_key(message.message_id, AbortMode.CANCEL)):
            raise SkipMessage()

        self.abortables[message.message_id] = threading.get_ident()

    def after_process_message(
        self,
        broker: dramatiq.Broker,
        message: dramatiq.Message,
        *,
        result: Optional[Any] = None,
        exception: Optional[BaseException] = None
    ) -> None:
        with self.lock:
            self.abortables.pop(message.message_id, None)

    after_skip_message = after_process_message

    def abort(self, message_id: str, mode: AbortMode = AbortMode.ABORT) -> None:
        if mode is AbortMode.ABORT:
            self.backend.notify(
                [
                    self.id_to_key(message_id, AbortMode.CANCEL),
                    self.id_to_key(message_id, AbortMode.ABORT),
                ],
                ttl=self.abort_ttl,
            )
        elif mode is AbortMode.CANCEL:
            self.backend.notify([self.id_to_key(message_id, mode)], ttl=self.abort_ttl)

    def _handle(self) -> None:
        message_ids = list(self.abortables.keys())
        if not message_ids:
            time.sleep(self.wait_timeout / 1000)
            return

        abort_keys = [self.id_to_key(id_, AbortMode.ABORT) for id_ in message_ids]
        key = self.backend.wait_many(abort_keys, self.wait_timeout)
        if not key:
            return
        else:
            self.backend.poll(key)

        message_id = self.key_to_id(key, AbortMode.ABORT)
        with self.lock:
            thread_id = self.abortables.pop(message_id, None)
            # In case the task was done in between the polling and now.
            if thread_id is None:
                return  # pragma: no cover

            self.logger.info(
                "Aborting task. Raising exception in worker thread %r.", thread_id
            )
            raise_thread_exception(thread_id, Abort)

    def _watcher(self) -> None:
        while True:
            try:
                self._handle()
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Unhandled error while running the time limit handler."
                )

    @staticmethod
    def id_to_key(message_id: str, mode: AbortMode = AbortMode.ABORT) -> bytes:
        return (
            ("abort:" if mode is AbortMode.ABORT else "cancel:") + message_id
        ).encode()

    @staticmethod
    def key_to_id(key: bytes, mode: AbortMode = AbortMode.ABORT) -> str:
        return key.decode()[6:] if mode is AbortMode.ABORT else key.decode()[7:]


def abort(
    message_id: str,
    middleware: Optional[Abortable] = None,
    mode: AbortMode = AbortMode.ABORT,
) -> None:
    """Abort a pending or running message given its ``message_id``.

    :param message_id: Message to abort. Use the return value of ``actor.send``
        or ``actor.send_with_options`` to then use its ``.message_id`` attribute.

    :param middleware: :class:`Abortable` middleware used by the workers and
        broker used to signal termination. If set to ``None``, use the default broker
        from ``dramatiq.get_broker()`` and retrieve the configured :class:`Abortable`
        middleware. If no :class:`Abortable` middleware is set on the broker and
        ``middleware`` is ``None``, raises a :class:`RuntimeError`.

    :param mode: "abort" or "cancel".
        In "cancel" mode, only pending message will be aborted,
        running message will also be aborted additionally in "abort" mode.
    :type middleware: :class:`Abortable`
    """
    if not middleware:
        broker = get_broker()
        for middleware in broker.middleware:
            if isinstance(middleware, Abortable):
                break
        else:
            raise RuntimeError("The default broker doesn't have an abortable backend.")

    mode = AbortMode(mode)

    middleware.abort(message_id, mode)
