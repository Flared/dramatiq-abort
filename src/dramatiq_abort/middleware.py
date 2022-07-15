import time
import warnings
from enum import Enum
from threading import Thread
from typing import Any, Dict, List, Optional, Set, Tuple

import dramatiq
from dramatiq import get_broker
from dramatiq.logging import get_logger
from dramatiq.middleware import Middleware, SkipMessage
from dramatiq.middleware.threading import current_platform, supported_platforms

from .abort_manager import AbortManager, CtypesAbortManager, is_gevent_active
from .backend import Event, EventBackend


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
        self.manager: AbortManager
        if is_gevent_active():
            from .abort_manager import GeventAbortManager

            self.manager = GeventAbortManager(self.logger)
        else:
            self.manager = CtypesAbortManager(self.logger)

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

        event = self.backend.poll(self.id_to_key(message.message_id, AbortMode.CANCEL))
        if event:
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
        events = [Event(self.id_to_key(message_id, mode), args) for mode, args in modes]
        self.backend.notify(events, ttl=abort_ttl)

    def get_abort_request(self, message_id: Optional[str] = None) -> Optional[float]:
        abort_time = self.manager.get_abort_request(message_id)
        if abort_time is None:
            return None
        return 1000 * (abort_time - time.monotonic())

    def _get_abort_requests(self) -> None:
        message_ids = self.manager.get_abortables()
        if not message_ids:
            time.sleep(self.wait_timeout / 1000)
            return

        abort_keys = [self.id_to_key(id_, AbortMode.ABORT) for id_ in message_ids]
        event = self.backend.wait_many(abort_keys, self.wait_timeout)
        if not event:
            return

        message_id = self.key_to_id(event.key)
        self.manager.add_abort_request(message_id, **event.params)

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

    :param mode: "AbortMode.ABORT" or "AbortMode.CANCEL". In "cancel" mode,
        only pending message will be aborted,
        running message will also be aborted additionally in "abort" mode.

    :param abort_timeout: Only applicable when mode is "AbortMode.ABORT".
        If set, signals the running message that an abort is requested and waits for the
        given number of milliseconds for it to finish before aborting it.
        Messages can check if an abort is requested by calling :meth:`abort_requested`.
    """
    if not middleware:
        middleware = _get_abortable_from_broker()

    middleware.abort(message_id, abort_ttl, mode, abort_timeout)


def abort_requested(
    message_id: Optional[str] = None,
    middleware: Optional[Abortable] = None,
) -> Optional[float]:
    """Check if there is an abort request for the current message. Returns the number of
    milliseconds until the message is aborted via an exception or ``None`` if no abort
    is requested.

    :param message_id: If provided, checks for a abort request for the given
        ``message_id`` instead of the current message.

    :param middleware: As in :meth:`abort`.
    """
    if not middleware:
        middleware = _get_abortable_from_broker()
    return middleware.get_abort_request(message_id)
