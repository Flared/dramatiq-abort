# dramatiq-abort

Add the aborting feature to [dramatiq] through a simple middleware with flexible backend.

Current version support aborting using the [Redis] store.

[![Build Status](https://github.com/Flared/dramatiq-abort/workflows/Push/badge.svg)](https://github.com/Flared/dramatiq-abort/actions?query=workflow%3A%22Push%22)
[![PyPI version](https://badge.fury.io/py/dramatiq-abort.svg)](https://badge.fury.io/py/dramatiq-abort)
[![Documentation](https://img.shields.io/badge/doc-latest-brightgreen.svg)](http://flared.github.io/dramatiq-abort)

## Installation

Since the only available backend right now is [Redis]:

    pip install dramatiq_abort[redis]

**Documentation**: http://flared.github.io/dramatiq-abort


## Quickstart

```python

from dramatiq import get_broker
from dramatiq_abort import Abortable, backends, abort

abortable = Abortable(backend=backends.RedisBackend())
get_broker().add_middleware(abortable)

# ...

import dramatiq

@dramatiq.actor
def my_long_running_task(): ...

message = my_long_running_task.send()

# Now abort the message.
abort(message.message_id)
```

[Redis]: https://redis.io
[dramatiq]: https://dramatiq.io/

