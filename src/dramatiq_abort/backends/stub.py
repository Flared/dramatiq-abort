from threading import Condition
from typing import Iterable, List, Optional, Set

from ..backend import EventBackend


class StubBackend(EventBackend):
    def __init__(self) -> None:
        self.condition = Condition()
        self.events: Set[bytes] = set()

    def wait_many(self, keys: List[bytes], timeout: int) -> Optional[bytes]:
        with self.condition:
            if self.condition.wait_for(
                lambda: self._anyset(keys), timeout=timeout / 1000
            ):
                for key in keys:
                    if key in self.events:
                        self.events.remove(key)
                        return key
        return None

    def poll(self, key: bytes) -> bool:
        with self.condition:
            if key in self.events:
                self.events.remove(key)
                return True
        return False

    def notify(self, keys: Iterable[bytes], ttl: int) -> None:
        with self.condition:
            self.events.update(keys)
            self.condition.notify_all()

    def notify_many(self, keys: List[bytes], ttl: int) -> None:
        with self.condition:
            self.events.update(keys)
            self.condition.notify_all()

    def _anyset(self, keys: List[bytes]) -> bool:
        return any(k in self.events for k in keys)
