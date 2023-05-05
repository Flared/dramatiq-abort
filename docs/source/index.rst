.. dramatiq-abort documentation master file, created by
  sphinx-quickstart on Thu Dec 26 14:09:29 2019.
  You can adapt this file completely to your liking, but it should at least
  contain the root `toctree` directive.

.. highlight:: python

dramatiq-abort
==============

.. toctree::

  Documentation <http://packages.python.org/dramatiq-abort/>
  Source <http://github.com/Flared/dramatiq-abort>

.. module:: dramatiq-abort

When running `dramatiq`_ task, it might be useful to abort enqueued or even
running actors.

The **dramatiq-abort** middleware allows to signal a task termination from any
dramatiq client using a distributed event backend. At the moment, only a Redis
backend exists, but the API provide a simple interface to add more.

Installing dramatiq-abort
-------------------------

Install with **pip**::

    pip install dramatiq-abort[redis]

Configuring dramatiq-abort
--------------------------

**dramatiq-abort** is configured by creating a backend object and adding the
``Abortable`` middleware to the **dramatiq** ``Broker`` instance.

.. code-block::

  from dramatiq import get_broker
  from dramatiq_abort import Abortable, backends

  event_backend = backends.RedisBackend()
  abortable = Abortable(backend=event_backend)
  get_broker.add_middleware(abortable)

Abort a task
------------

When an actor is sent to a worker, a message instance is returned with the
message id. This message id can be kept somewhere so it can be used to abort
the enqueued or running task. :any:`abort` can then be used to signal the task
termination using the message id in two modes:

- `cancel` mode only aborts the enqueued but pending task.
- `abort` mode will abort the pending or running task.

.. code-block::

  @dramatiq.actor
  def my_long_running_task(): ...

  message = my_long_running_task.send()
  message_id = message.message_id

  # the default mode is 'AbortMode.ABORT'
  abort(message_id)
  abort(message_id, mode=AbortMode.CANCEL)

Gracefully aborting tasks
-------------------------

When the abort mode is set to :any:`abort`, an optional timeout can be provided to allow
for tasks to finish before being aborted.
Running tasks can check if an abort is requested by calling :any:`abort_requested`,
which returns the number of milliseconds until the task is aborted or ``None`` if no
abort is requested.

.. code-block::

  @dramatiq.actor
  def my_long_running_task():
    while True:
      sleep(0.1) # do work
      if abort_requested():
        break # stop working

  message = my_long_running_task.send()

  # signals the task that an abort is requested, allowing 2 seconds for it to finish
  abort(message.message_id, mode=AbortMode.ABORT, abort_timeout=2000)

Abort a task using a custom abort_ttl value
-------------------------------------------

By default, abort has a limited window of 90,000 milliseconds. This means a worker will
skip a task only if the task was aborted up to 90 seconds ago. In case of longer delay
in the task processing this value can be overridden.

.. code-block::

  @dramatiq.actor
  def count_words(url): ...

  message = count_words.send_with_options(args=("https://example.com",), delay=60*60*1000)
  message_id = message.message_id

  abort(message_id, abort_ttl=2*60*60*1000)

Abort a task running a subprocess
---------------------------------
When a worker is waiting on a subprocess, the :any:`Abort` exception will only be raised 
AFTER the subprocess has been completed. The following example is one way to deal with this:

.. code-block::
  @dramatiq.actor
  def my_subprocess():
    with subprocess.Popen(*args, **kwargs) as process:
      ret = None
      try:
        while ret is None:
          try:
            ret = process.wait(timeout=1)  # Note: same principle for `process.communicate()`
          except subprocess.TimeoutExpired:
            pass
      except Abort:
        process.signal(signal.SIGINT)  # or on windows: `os.kill(process.pid, signal.SIGINT)`
        process.wait()

API
---

.. module:: dramatiq_abort

.. autofunction:: abort

.. autofunction:: abort_requested

.. autoclass:: Abortable

.. autoclass:: Abort

.. autoclass:: Event

.. autoclass:: EventBackend
   :members:

.. module:: dramatiq_abort.backends

.. autoclass:: RedisBackend
   :members: from_url

.. _dramatiq: https://dramatiq.io
.. _GitHub: https://github.com/Flared/dramatiq-abort
