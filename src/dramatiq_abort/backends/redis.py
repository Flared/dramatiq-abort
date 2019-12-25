from typing import Any, List, Optional

import redis

from ..backend import EventBackend


class RedisBackend(EventBackend):
    """A event backend for Redis_.

    Parameters:
      client(Redis): An optional client.  If this is passed,
        then all other parameters are ignored.
      url(str): An optional connection URL.  If both a URL and
        connection paramters are provided, the URL is used.
      **parameters(dict): Connection parameters are passed directly
        to :class:`redis.Redis`.

    .. _redis: https://redis.io
    """

    def __init__(self, *, client: Any) -> None:
        self.client = client

    @classmethod
    def from_url(cls, url: str) -> "RedisBackend":
        return cls(
            client=redis.StrictRedis(connection_pool=redis.ConnectionPool.from_url(url))
        )

    def wait_many(self, keys: List[bytes], timeout: int) -> Optional[bytes]:
        assert timeout is None or timeout >= 1000, "wait timeouts must be >= 1000"
        event = self.client.blpop(keys, (timeout or 0) // 1000)
        if event is None:
            return None
        key, value = event
        if value != b"x":
            return None
        return key

    def poll(self, key: bytes) -> bool:
        event = self.client.lpop(key)
        return event == b"x"

    def notify(self, key: bytes, ttl: int) -> None:
        with self.client.pipeline() as pipe:
            pipe.rpush(key, b"x")
            pipe.pexpire(key, ttl)
            pipe.execute()
