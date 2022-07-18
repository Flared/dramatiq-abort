import json
from typing import Any, Dict, Iterable, Optional, Tuple

import redis

from ..backend import Event, EventBackend

DEFAULT_NAMESPACE = "dramatiq:aborts:"


class RedisBackend(EventBackend):
    """An event backend for Redis_.

    :param client: The `Redis client`_ instance.
    :param namespace: Namespace to prefix keys with.

    .. _redis: https://redis.io
    .. _`redis client`: https://pypi.org/project/redis/
    """

    def __init__(self, *, client: Any, namespace: str = DEFAULT_NAMESPACE) -> None:
        self.client = client
        self.namespace = namespace

    @classmethod
    def from_url(cls, url: str, *args: Any, **kwargs: Any) -> "RedisBackend":
        """Initialize the backend using an URL to the Redis server.
        Any extra parameters are passed on to the default constructor.

        :param url: Redis server URL.
        """
        return cls(
            *args,
            client=redis.StrictRedis(
                connection_pool=redis.ConnectionPool.from_url(url)
            ),
            **kwargs,
        )

    def wait_many(self, keys: Iterable[str], timeout: int) -> Optional[Event]:
        assert timeout is None or timeout >= 1000, "wait timeouts must be >= 1000"
        event: Optional[Tuple[bytes, Optional[bytes]]]
        event = self.client.blpop(
            [self._encode_key(key) for key in keys], (timeout or 0) // 1000
        )
        if not event:
            return None
        key, value = self._decode_key(event[0]), self._decode_value(event[1])
        if value is None:
            return None
        return Event(key, value)

    def poll(self, key: str) -> Optional[Event]:
        encoded_value: Optional[bytes] = self.client.lpop(self._encode_key(key))
        value = self._decode_value(encoded_value)
        if value is None:
            return None
        return Event(key, value)

    def notify(self, events: Iterable[Event], ttl: int) -> None:
        with self.client.pipeline() as pipe:
            for key, val in events:
                pipe.rpush(self._encode_key(key), self._encode_value(val))
                pipe.pexpire(self._encode_key(key), ttl)
            pipe.execute()

    def _encode_key(self, key: str) -> bytes:
        return (self.namespace + key).encode()

    @staticmethod
    def _encode_value(value: Dict[str, Any]) -> bytes:
        if not isinstance(value, dict):  # pragma: no cover
            raise TypeError("Must provide a dict in params")
        return json.dumps(value).encode()

    def _decode_key(self, key: bytes) -> str:
        return key.decode()[len(self.namespace) :]

    @staticmethod
    def _decode_value(value: Optional[bytes]) -> Optional[Dict[str, Any]]:
        if not value:
            return None
        try:
            value = json.loads(value)
            if not isinstance(value, dict):
                return None
            return value
        except (UnicodeError, json.JSONDecodeError):
            return None
