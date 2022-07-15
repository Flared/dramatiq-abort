from threading import Condition
from typing import Any, Dict, Iterable, Optional, Tuple

from ..backend import EventBackend


class StubBackend(EventBackend):
    def __init__(self) -> None:
        self.condition = Condition()
        self.events: Dict[str, Dict[str, Any]] = dict()

    def wait_many(
        self, keys: Iterable[str], timeout: int
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        with self.condition:
            if self.condition.wait_for(
                lambda: self._anyset(keys), timeout=timeout / 1000
            ):
                for key in keys:
                    if key in self.events:
                        return key, self.events.pop(key)
        return None, None

    def poll(self, key: str) -> Optional[Dict[str, Any]]:
        with self.condition:
            return self.events.pop(key, None)

    def notify(self, items: Iterable[Tuple[str, Dict[str, Any]]], ttl: int) -> None:
        with self.condition:
            for k, v in items:
                self.events[k] = v
            self.condition.notify_all()

    def _anyset(self, keys: Iterable[str]) -> bool:
        return any(k in self.events for k in keys)
