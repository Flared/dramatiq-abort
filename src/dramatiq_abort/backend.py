import abc
from typing import List, Optional


class EventBackend(abc.ABC):
    """ABC for event backends.
    """

    @abc.abstractmethod
    def wait_many(
        self, keys: List[bytes], timeout: int
    ) -> Optional[bytes]:  # pragma: no cover
        """Wait for either one of the event in ``keys`` to be signaled or
        ``timeout`` milliseconds to elapsed.

        Returns the event that signaled or ``None`` if no event was signaled.
        A backend might not be idempotent and once a key has signaled,
        subsequent calls might wait indefinitely.


        :param keys: List of event to wait for.
        :param timeout: Maximum amount of milliseconds to wait.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def poll(self, key: bytes) -> bool:  # pragma: no cover
        """Check if an event has been signaled.

        This function should not block and wait for an event to signal.
        Returns ``True`` if the event was signaled, ``False`` otherwise.
        A backend might not be idempotent and once a key has signaled,
        subsequent call returns ``False``.

        :param key: Event to check for signal.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def notify(self, key: bytes, ttl: int) -> None:  # pragma: no cover
        """Signal an event.

        Once notified, a call to :any:`poll` or :any:`wait_many` with this event should
        result in a positive result.

        :param key: Event to signal.
        :param ttl: Time for the signal to live. The value should be large
            enough to give time for workers to poll the value, but small enough
            that the backend doesn't end up with too many outdated keys not
            being garbage collected.
        """
        raise NotImplementedError
