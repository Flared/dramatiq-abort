import logging
import os
import random
from typing import Any, Dict

import dramatiq
import pytest
import redis
from dramatiq import Worker
from dramatiq.brokers.stub import StubBroker

from dramatiq_abort import EventBackend
from dramatiq_abort import backends as evt_backends

logfmt = "[%(asctime)s] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=logfmt)
logging.getLogger("pika").setLevel(logging.WARN)

random.seed(1337)

CI: bool = os.getenv("GITHUB_ACTION") is not None


def check_redis(client: redis.StrictRedis) -> None:
    try:
        client.ping()
    except redis.ConnectionError as e:
        raise e if CI else pytest.skip("No connection to Redis server.")


@pytest.fixture()
def stub_broker() -> dramatiq.Broker:
    broker = StubBroker()
    broker.emit_after("process_boot")
    dramatiq.set_broker(broker)
    yield broker
    broker.flush_all()
    broker.close()


@pytest.fixture()
def stub_worker(stub_broker: dramatiq.Broker) -> dramatiq.Worker:
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=32)
    worker.start()
    yield worker
    worker.stop()


@pytest.fixture
def redis_event_backend() -> evt_backends.RedisBackend:
    backend = evt_backends.RedisBackend.from_url("redis://localhost:6379")
    check_redis(backend.client)
    backend.client.flushall()
    return backend


@pytest.fixture
def stub_event_backend() -> evt_backends.StubBackend:
    return evt_backends.StubBackend()


@pytest.fixture
def event_backends(
    redis_event_backend: evt_backends.RedisBackend,
    stub_event_backend: evt_backends.StubBackend,
) -> Dict[str, EventBackend]:
    return {
        "redis": redis_event_backend,
        "stub": stub_event_backend,
    }


@pytest.fixture(params=["redis", "stub"])
def event_backend(
    request: Any, event_backends: Dict[str, EventBackend]
) -> EventBackend:
    return event_backends[request.param]
