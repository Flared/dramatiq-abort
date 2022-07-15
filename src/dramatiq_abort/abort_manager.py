import abc
import contextlib
import threading
import time
from logging import Logger
from threading import Thread
from typing import Any, ContextManager, Dict, List, NamedTuple, Optional

from dramatiq.logging import get_logger
from dramatiq.middleware.threading import Interrupt, raise_thread_exception


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


class AbortRequest(NamedTuple):
    message_id: str
    abort_time: float


class AbortManager(abc.ABC):
    """ABC for raising Abort exceptions in threads.

    :param logger: The logger for abort log lines.
    :type logger: :class:`logging.Logger`
    """

    def __init__(self, logger: Optional[Logger] = None):
        self.logger = logger or get_logger(__name__, type(self))
        # message_id -> thread
        self.abortable_messages: Dict[str, Any] = {}
        # thread -> (message_id, abort_time)
        self.abort_requests: Dict[Any, AbortRequest] = {}

    @abc.abstractmethod
    def get_current_thread(self) -> Any:  # pragma: no cover
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def lock(self) -> ContextManager[Any]:  # pragma: no cover
        # This lock is used to avoid tasks finishing while they are being aborted
        raise NotImplementedError

    @abc.abstractmethod
    def do_abort(self, thread: Any) -> None:  # pragma: no cover
        raise NotImplementedError

    def add_abortable(self, message_id: str) -> None:
        self.abortable_messages[message_id] = self.get_current_thread()

    def remove_abortable(self, message_id: str) -> None:
        with self.lock:
            thread = self.abortable_messages.pop(message_id, None)
            if thread:
                abort_request = self.abort_requests.pop(thread, None)
                assert not abort_request or message_id == abort_request.message_id

    def get_abortables(self) -> List[str]:
        return list(self.abortable_messages.keys())

    def get_abort_request(self, message_id: Optional[str] = None) -> Optional[float]:
        if message_id:  # pragma: no cover
            thread = self.abortable_messages.get(message_id, None)
        else:
            thread = self.get_current_thread()
        abort_request = self.abort_requests.get(thread, None)
        if abort_request:
            return abort_request.abort_time
        return None

    def add_abort_request(self, message_id: str, abort_timeout: int = 0) -> None:
        with self.lock:
            thread = self.abortable_messages.get(message_id, None)
            if thread is None:  # pragma: no cover
                # If the task finished before we signaled it to abort
                return
            self.abort_requests[thread] = AbortRequest(
                message_id=message_id,
                abort_time=time.monotonic() + abort_timeout / 1000,
            )

    def abort_pending(self) -> None:
        with self.lock:
            toabort = [
                thread
                for thread, (_, abort_time) in self.abort_requests.items()
                if time.monotonic() >= abort_time
            ]

            for thread in toabort:
                abort_request = self.abort_requests.pop(thread, None)
                assert abort_request
                saved_thread = self.abortable_messages.pop(
                    abort_request.message_id, None
                )
                assert saved_thread == thread

                self.logger.info(
                    "Aborting task. Raising exception in worker thread %r.", thread
                )
                self.do_abort(thread)


class CtypesAbortManager(AbortManager):
    """Manager for raising Abort exceptions via the ctypes api."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._lock = threading.Lock()

    @property
    def lock(self) -> threading.Lock:
        return self._lock

    def get_current_thread(self) -> Thread:
        return threading.current_thread()

    def do_abort(self, thread: Thread) -> None:
        raise_thread_exception(thread.ident, Abort)


if is_gevent_active():
    from gevent import Greenlet, getcurrent

    class GeventAbortManager(AbortManager):
        """Manager for raising Abort exceptions in green threads."""

        def __init__(self, *args: Any, **kwargs: Any):
            super().__init__(*args, **kwargs)
            # No lock is needed for gevent
            self._lock = contextlib.nullcontext()

        @property
        def lock(self) -> ContextManager[None]:
            return self._lock

        def get_current_thread(self) -> Greenlet:
            return getcurrent()

        def do_abort(self, thread: Greenlet) -> None:
            thread.kill(Abort, block=False)
