===================
Light PgQ Framework
===================

This module provides a convenient Python API to integrate
PostgreSQL PgQ features with any Python application.


Presentation of PgQ
-------------------

*(from SkyTools README)*

PgQ is a queuing system written in PL/pgSQL, Python and C code.  It is
based on snapshot-based event handling ideas from Slony-I, and is
written for general usage.

PgQ provides an efficient, transactional queueing system with
multi-node support (including work sharing and splitting, failover and
switchover, for queues and for consumers).

Rules:

- There can be several queues in a database.
- There can be several producers than can insert into any queue.
- There can be several consumers on one queue.
- There can be several subconsumers on a consumer.

PgQ is split into 3 layers: Producers, Ticker and Consumers.

**Producers** and **Consumers** respectively push and read events into
a queue.  Producers just need to call PostgreSQL stored procedures
(like a trigger on a table or a PostgreSQL call from the application).
Consumers are frequently written in Python, but any language able to
run PostgreSQL stored procedures can be used.

**Ticker** is a daemon which splits the queues into batches of events and
handle the maintenance of the system.


The PgQueue module
------------------

This module provides Python functions and classes to write **Producers**
and **Consumers**.
It contains also a Python implementation of the **Ticker** engine, which
mimics the original C Ticker from SkyTools: it splits batches of events,
and execute maintenance tasks.


Installation
------------

The ``pgq`` extension is required on the PostgreSQL server.

On Debian / Ubuntu you will add `the APT repository
<https://wiki.postgresql.org/wiki/Apt>`_, then install the package
``postgresql-9.x-pgq3`` depending on the PostgreSQL version.
Finally create the extension in the database:

::

  CREATE EXTENSION IF NOT EXISTS pgq;

You can install the ``pgqueue`` module into your environment.

::

  pip install --update pgqueue


Example usage
-------------

You need to run the **Ticker** permanently.
If the Ticker is off, the events will be stored into the queues,
but no batch will be prepared for the consumers, and event tables will
grow quickly.

For the Ticker, you have the choice between the optimized ``pgqd``
multi-database ticker written in C, and part of SkyTools, or use the
simpler Python implementation provided with this module:

::

  python -m pgqueue 'host=127.0.0.1 port=5432 user=jules password=xxxx dbname=test_db'

Let's create a new queue, and register a consumer:

::

  conn = psycopg2.connect("dbname=test user=postgres")
  conn.autocommit = True
  cur = conn.cursor()
  first_q = Queue('first_queue')
  first_q.create(cursor, ticker_max_lag='4 seconds')
  consum_q = Consumer('first_queue', 'consumer_one')
  consum_q.register(cursor)


We're ready to produce events into the queue, and consume events
later in the application:

::

  first_q.insert_event(cursor, 'announce', 'Hello ...')
  first_q.insert_event(cursor, 'announce', 'Hello world!')
  # ... wait a little bit
  conn.autocommit = False
  for event in consum_q.next_events(cursor, commit=True):
      print(event)

You can browse the source code for advanced usage, until we write
more documentation (contributions are welcomed).

Also refer to `the upstream documentation for more details
<http://skytools.projects.pgfoundry.org/skytools-3.0/>`_.


Credits
-------

PgQ is a PostgreSQL extension which is developed by Marko Kreen.
It is part of SkyTools, a package of tools in use in Skype for
replication and failover.

SkyTools embeds also a ``pgq`` Python framework which provides a
slightly different API.
