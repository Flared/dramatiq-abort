import abc
from typing import List, Optional


class EventBackend(abc.ABC):
    """ABC for event backends.
    """

    @abc.abstractmethod
    def wait_many(
        self, keys: List[bytes], timeout: int
    ) -> Optional[bytes]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def poll(self, key: bytes) -> bool:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def notify(self, key: bytes, ttl: int) -> None:  # pragma: no cover
        raise NotImplementedError
