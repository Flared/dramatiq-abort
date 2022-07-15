from threading import Condition
from typing import Any, Dict, Iterable, Optional

from ..backend import Event, EventBackend


class StubBackend(EventBackend):
    def __init__(self) -> None:
        self.condition = Condition()
        self.events: Dict[str, Dict[str, Any]] = dict()

    def wait_many(self, keys: Iterable[str], timeout: int) -> Optional[Event]:
        with self.condition:
            if self.condition.wait_for(
                lambda: self._anyset(keys), timeout=timeout / 1000
            ):
                for key in keys:
                    if key in self.events:
                        return Event(key, self.events.pop(key))
        return None

    def poll(self, key: str) -> Optional[Event]:
        with self.condition:
            if key not in self.events:
                return None
            return Event(key, self.events.pop(key))

    def notify(self, events: Iterable[Event], ttl: int) -> None:
        with self.condition:
            for k, v in events:
                self.events[k] = v
            self.condition.notify_all()

    def _anyset(self, keys: Iterable[str]) -> bool:
        return any(k in self.events for k in keys)
