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
termination using the message id.

.. code-block::

  @dramatiq.actor
  def my_long_running_task(): ...

  message = my_long_running_task.send()
  message_id = message.message_id

  abort(message_id)

Abort a task using a custom abort_ttl value
-------------------------------------------

By default, abort has a limited window of 90,000 milliseconds. This means a worker will skip a task only if the task was aborted up to 90 seconds ago. In case of longer delay in the task processing this value can be overridden.

.. code-block::

  @dramatiq.actor
  def count_words(url): ...

  message = count_words.send_with_options(args=("https://example.com",), delay=60*60*1000)
  message_id = message.message_id

  abort(message_id, abort_ttl=2*60*60*1000)

API
---

.. module:: dramatiq_abort

.. autofunction:: abort

.. autoclass:: Abortable

.. autoclass:: Abort

.. autoclass:: EventBackend
   :members:

.. module:: dramatiq_abort.backends

.. autoclass:: RedisBackend
   :members: from_url

.. _dramatiq: https://dramatiq.io
.. _GitHub: https://github.com/Flared/dramatiq-abort
