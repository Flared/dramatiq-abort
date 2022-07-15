import json
from typing import Any, Dict, Iterable, Optional, Tuple, Union, overload

import redis

from ..backend import EventBackend

MARKER = "dramatiq-abort_marker"
DEFAULT_NAMESPACE = "dramatiq:abort:"


class RedisBackend(EventBackend):
    """An event backend for Redis_.

    :param client: The `Redis client`_ instance.

    .. _redis: https://redis.io
    .. _`redis client`: https://pypi.org/project/redis/
    """

    def __init__(self, *, client: Any, namespace: Optional[str] = None) -> None:
        self.client = client
        self.namespace = namespace or DEFAULT_NAMESPACE

    @classmethod
    def from_url(cls, url: str, *args: Any, **kwargs: Any) -> "RedisBackend":
        """Initialize the backend using an URL to the Redis server.

        :param url: Redis server URL.
        """
        return cls(
            *args,
            client=redis.StrictRedis(
                connection_pool=redis.ConnectionPool.from_url(url)
            ),
            **kwargs,
        )

    def wait_many(
        self, keys: Iterable[str], timeout: int
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        assert timeout is None or timeout >= 1000, "wait timeouts must be >= 1000"
        event: Optional[Tuple[bytes, Optional[bytes]]]
        event = self.client.blpop(
            [self._encode(key=key) for key in keys], (timeout or 0) // 1000
        )
        if not event:
            return None, None
        return self._decode(key=event[0], value=event[1])

    def poll(self, key: str) -> Optional[Dict[str, Any]]:
        event: Optional[bytes] = self.client.lpop(self._encode(key=key))
        return self._decode(value=event)

    def notify(self, items: Iterable[Tuple[str, Dict[str, Any]]], ttl: int) -> None:
        with self.client.pipeline() as pipe:
            for key, val in items:
                pipe.rpush(*self._encode(key=key, value=val))
                pipe.pexpire(key, ttl)
            pipe.execute()

    def _encode(
        self, *, key: Optional[str], value: Optional[Dict[str, Any]] = None
    ) -> Union[None, bytes, Tuple[bytes, bytes]]:
        encoded_key = (self.namespace + key).encode() if key else None
        encoded_value = (
            json.dumps((MARKER, value)).encode() if value is not None else None
        )

        if encoded_key and encoded_value:
            return encoded_key, encoded_value
        elif encoded_value:
            return encoded_value
        return encoded_key

    @overload
    def _decode(self, *, key: Optional[bytes]) -> Optional[str]:
        ...

    @overload
    def _decode(self, *, value: Optional[bytes]) -> Optional[Dict[str, Any]]:
        ...

    @overload
    def _decode(
        self, *, key: bytes, value: Optional[bytes]
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        ...

    def _decode(
        self, *, key: Optional[bytes] = None, value: Optional[bytes] = None
    ) -> Union[None, str, Dict[str, Any], Tuple[str, Optional[Dict[str, Any]]]]:
        decoded_key = key.decode()[len(self.namespace) :] if key else None
        decoded_value = self._decode_value(value) if value else None
        if decoded_key and decoded_value:
            return decoded_key, decoded_value
        elif decoded_value:
            return decoded_value
        return decoded_key

    @staticmethod
    def _decode_value(val: bytes) -> Optional[Dict[str, Any]]:
        try:
            val = json.loads(val)
            if (
                not isinstance(val, list)
                or len(val) != 2
                or val[0] != MARKER
                or not isinstance(val[1], dict)
            ):
                return None
            return val[1]
        except (UnicodeError, json.JSONDecodeError):
            return None
