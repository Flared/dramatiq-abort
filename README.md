# dramatiq-abort

Add the aborting feature to [dramatiq] through a simple middleware with flexible backend.

Current version support aborting using the [Redis] store.


## Installation

Since the only available backend right now is [Redis]:

    pip install dramatiq_abort[redis]


## Quickstart

```python

from dramatiq import get_broker
from dramatiq_abort import Abortable, backends, abort

abortable = Abortable(backend=backends.RedisBackend())
get_broker.add_middleware(abortable)

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

