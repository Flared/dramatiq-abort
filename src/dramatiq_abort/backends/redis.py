from typing import Any, Iterable, List, Optional, Tuple

import redis

from ..backend import EventBackend


class RedisBackend(EventBackend):
    """An event backend for Redis_.

    :param client: The `Redis client`_ instance.

    .. _redis: https://redis.io
    .. _`redis client`: https://pypi.org/project/redis/
    """

    def __init__(self, *, client: Any) -> None:
        self.client = client

    @classmethod
    def from_url(cls, url: str) -> "RedisBackend":
        """Initialize the backend using an URL to the Redis server.

        :param url: Redis server URL.
        """
        return cls(
            client=redis.StrictRedis(connection_pool=redis.ConnectionPool.from_url(url))
        )

    def wait_many(self, keys: List[bytes], timeout: int) -> Optional[bytes]:
        assert timeout is None or timeout >= 1000, "wait timeouts must be >= 1000"
        event: Optional[Tuple[bytes, bytes]] = self.client.blpop(
            keys, (timeout or 0) // 1000
        )
        if event is None:
            return None
        key, value = event
        if value != b"x":
            return None
        return key

    def poll(self, key: bytes) -> bool:
        event: Optional[bytes] = self.client.lpop(key)
        return event == b"x"

    def notify(self, keys: Iterable[bytes], ttl: int) -> None:
        with self.client.pipeline() as pipe:
            for key in keys:
                pipe.rpush(key, b"x")
                pipe.pexpire(key, ttl)
            pipe.execute()
