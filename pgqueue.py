# -*- coding: utf-8 -*-
"""Light PgQ framework.

Stripped from Skytools 3.
"""
import logging
import time
from operator import itemgetter

import psycopg2
import psycopg2.extras

__version__ = '0.5'
__all__ = ['Event', 'Batch', 'Consumer', 'Queue', 'Ticker',
           'bulk_insert_events', 'insert_event']


class _DisposableFile(dict):
    """Awfully limited file API for psycopg2 copy_from."""
    __slots__ = ()

    def read(self, size=-1):
        return self.pop('content', '')
    readline = read


def quote_ident(s):
    """Quote SQL identifier."""
    return ('"%s"' % s.replace('"', '""')) if s else '""'


def quote_copy(s, _special='\n\\\t\r'):
    r"""Quote for COPY FROM command.  None is converted to \N."""
    if s is None:
        return r'\N'
    s = str(s)
    for char in _special:
        if char in s:
            return (s.replace('\\', '\\\\')
                     .replace('\t', '\\t')
                     .replace('\n', '\\n')
                     .replace('\r', '\\r'))
    return s


def quote_dsn_param(s, _special=' \\\''):
    """Apply the escaping rule required by PQconnectdb."""
    if not s:
        return "''"
    for char in _special:
        if char in s:
            return "'%s'" % s.replace("\\", "\\\\").replace("'", "\\'")
    return s


def insert_event(curs, queue_name, ev_type, ev_data,
                 extra1=None, extra2=None, extra3=None, extra4=None):
    curs.execute("SELECT pgq.insert_event(%s, %s, %s, %s, %s, %s, %s);",
                 (queue_name, ev_type, ev_data,
                  extra1, extra2, extra3, extra4))
    return curs.fetchone()[0]


def bulk_insert_events(curs, queue_name, rows, columns):
    curs.execute("SELECT pgq.current_event_table(%s);", (queue_name,))
    event_tbl = curs.fetchone()[0]
    db_fields = ['ev_' + fld for fld in columns]
    content = ('\t'.join([quote_copy(v) for v in row]) for row in rows)
    df = _DisposableFile(content='\n'.join(content) + '\n')
    curs.copy_from(df, event_tbl, columns=db_fields)


class Event(tuple):
    """Event data for consumers.

    Consumer is supposed to tag them after processing.
    They will be removed from the queue by default.
    """
    __slots__ = ()
    _fields = ('id', 'txid', 'time', 'type', 'data',
               'extra1', 'extra2', 'extra3', 'extra4',
               'retry', '_failed')

    # Provide the event attributes as instance properties
    for _n, _attr in enumerate(_fields):
        locals()[_attr] = property(itemgetter(_n))
    del _n, _attr

    @property
    def __dict__(self):
        return dict(zip(self._fields, self))

    @property
    def failed(self):
        """Planned for retry?"""
        return self.id in self._failed

    @property
    def retry_time(self):
        """Interval before this event is retried.

        It returns None if this event is not planned for retry.
        """
        return self._failed.get(self.id)

    def tag_done(self):
        """Flag this event done (not necessary)."""
        if self.id in self._failed:
            del self._failed[self.id]

    def tag_retry(self, retry_time=60):
        """Flag this event for retry.

        It will be put back in queue and included in a future batch.
        """
        self._failed[self.id] = retry_time

    def __str__(self):
        return ("<id=%(id)d type=%(type)s data=%(data)s e1=%(extra1)s "
                "e2=%(extra2)s e3=%(extra3)s e4=%(extra4)s>" % self.__dict__)
    __repr__ = __str__


class Batch(object):
    """Lazy iterator over batch events.

    Events are loaded using cursor.
    It allows:

     - one for-loop over events
     - len() after that
    """

    _cursor_name = "batch_walker"
    # can retry events
    _retriable = True

    def __init__(self, curs, batch_id, queue_name,
                 fetch_size=300, predicate=None):
        self.queue_name = queue_name
        self.fetch_size = fetch_size
        self.batch_id = batch_id
        self.predicate = predicate
        self._curs = curs
        self.length = 0
        self.failed = {}        # {ev_id: retry_time}
        self.fetch_status = 0   # 0-not started, 1-in-progress, 2-done

    def _make_event(self, row):
        row['ev_retry'] = row['ev_retry'] or 0
        return Event([row['ev_' + fld] for fld in Event._fields[:-1]] +
                     [self.failed])

    def _fetch(self):
        q = "SELECT * FROM pgq.get_batch_events(%s)"
        if self.predicate:
            q += " WHERE %s" % self.predicate
        self._curs.execute(q, (self.batch_id,))
        self.length = self._curs.rowcount
        # Cursor is an iterable
        return self._curs

    def _fetchcursor(self):
        q = "SELECT * FROM pgq.get_batch_cursor(%s, %s, %s, %s);"
        self._curs.execute(q, (self.batch_id, self._cursor_name,
                               self.fetch_size, self.predicate))
        # this will return first batch of rows

        q = "FETCH %d FROM %s;" % (self.fetch_size, self._cursor_name)
        while True:
            rowcount = self._curs.rowcount
            if not rowcount:
                break

            self.length += rowcount
            for row in self._curs:
                yield row

            # if less rows than requested, it was final block
            if rowcount < self.fetch_size:
                break

            # request next block of rows
            self._curs.execute(q)

        self._curs.execute("CLOSE %s;" % self._cursor_name)

    def __iter__(self):
        if self.fetch_status:
            raise RuntimeError("Batch: double fetch? (%d)" % self.fetch_status)
        self.fetch_status = 1
        fetchall = self._fetchcursor if self.fetch_size else self._fetch
        for row in fetchall():
            yield self._make_event(row)
        self.fetch_status = 2

    def finish(self):
        """Tag events and notify that the batch is done."""
        if self.fetch_status >= 3:
            return  # already finished
        if self._retriable and self.failed:
            self._flush_retry()
        self._curs.execute("SELECT pgq.finish_batch(%s);", (self.batch_id,))
        self.fetch_status = 3

    def _flush_retry(self):
        """Tag retry events."""
        retried_events = ((self.batch_id, ev_id, retry_time)
                          for (ev_id, retry_time) in self.failed.items())
        self._curs.executemany("SELECT pgq.event_retry(%s, %s, %s);",
                               retried_events)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        # if partially processed, do not 'finish_batch'
        if exc_value is None and self.fetch_status == 2:
            self.finish()

    def __len__(self):
        return self.length

    def __bool__(self):
        return self.batch_id is not None
    __nonzero__ = __bool__  # Python 2

    def __repr__(self):
        return '<Batch %s:%d>' % (self.queue_name, self.batch_id)


class Queue(object):
    """Queue class."""

    def __init__(self, queue_name):
        if queue_name:
            self.queue_name = queue_name
        assert self.queue_name

    def create(self, curs, **params):
        """Create queue, if it does not exists."""
        curs.execute("SELECT pgq.create_queue(%s);",
                     (self.queue_name,))
        res = curs.fetchone()[0]
        for key, value in params.items():
            self.set_config(curs, key, value)
        return res

    def drop(self, curs, force=False):
        """Drop queue and all associated tables."""
        curs.execute("SELECT pgq.drop_queue(%s, %s);",
                     (self.queue_name, force))
        return curs.fetchone()[0]

    def set_config(self, curs, name, value):
        """Configure queue.

        Configurable parameters:
         - ticker_max_count     default 500
         - ticker_max_lag       default '3 seconds'
         - ticker_idle_period   default '1 minute'
         - ticker_paused        default False
         - rotation_period      default '2 hours'
         - external_ticker      default False
        """
        curs.execute("SELECT pgq.set_queue_config(%s, %s, %s);",
                     (self.queue_name, name, str(value)))
        return curs.fetchone()[0]

    def get_info(self, curs):
        """Get info about queue."""
        curs.execute("SELECT * FROM pgq.get_queue_info(%s);",
                     (self.queue_name,))
        return curs.fetchone()

    def register_consumer(self, curs, consumer, tick_id=None):
        """Register the consumer on the queue."""
        curs.execute("SELECT pgq.register_consumer_at(%s, %s, %s);",
                     (self.queue_name, str(consumer), tick_id))
        return curs.fetchone()[0]

    def unregister_consumer(self, curs, consumer):
        """Unregister the consumer from the queue."""
        curs.execute("SELECT pgq.unregister_consumer(%s, %s);",
                     (self.queue_name, str(consumer)))
        return curs.fetchone()[0]

    def get_consumer_info(self, curs, consumer=None):
        """Get info about consumer(s) on the queue."""
        if consumer and not isinstance(consumer, str):
            consumer = str(consumer)
        curs.execute("SELECT * FROM pgq.get_consumer_info(%s, %s);",
                     (self.queue_name, consumer))
        return curs.fetchone() if consumer else curs.fetchall()

    def insert_event(self, curs, ev_type, ev_data,
                     extra1=None, extra2=None, extra3=None, extra4=None):
        return insert_event(curs, self.queue_name, ev_type, ev_data,
                            extra1, extra2, extra3, extra4)

    def insert_events(self, curs, rows, columns):
        return bulk_insert_events(curs, self.queue_name, rows, columns)

    def external_tick(self, curs, tick_id, timestamp, event_seq):
        """External ticker.

        Insert a tick with a particular tick_id and timestamp.
        """
        curs.execute("SELECT pgq.ticker(%s, %s, %s, %s);",
                     (self.queue_name, tick_id, timestamp, event_seq))
        return curs.fetchone()[0]

    @classmethod
    def version(cls, curs):
        """Version string for PgQ."""
        curs.execute("SELECT pgq.version();")
        return curs.fetchone()[0]

    @classmethod
    def get_all_queues_info(cls, curs):
        """Get info about all queues."""
        curs.execute("SELECT * FROM pgq.get_queue_info();")
        return curs.fetchall()

    @classmethod
    def get_all_consumers_info(cls, curs):
        """Get info about all consumers on all queues."""
        curs.execute("SELECT * FROM pgq.get_consumer_info();")
        return curs.fetchall()


class Consumer(object):
    """Consumer base class."""
    # queue name to read from
    queue_name = None

    # consumer_name
    consumer_name = None

    # filter out only events for specific tables
    # predicate = "ev_extra1 IN ('table1', 'table2')"
    predicate = None

    # by default use cursor-based fetch (0 disables)
    pgq_lazy_fetch = 300

    # batch covers at least this many events
    pgq_min_count = None

    # batch covers at least this much time (PostgreSQL interval)
    pgq_min_interval = None

    # batch covers events older than that (PostgreSQL interval)
    pgq_min_lag = None
    # Note: pgq_min_lag together with pgq_min_interval/count is inefficient.

    _batch_class = Batch
    _queue_class = Queue
    _queue = None

    def __init__(self, queue_name=None, consumer_name=None, predicate=None):
        if queue_name:
            self.queue_name = queue_name
        if consumer_name:
            self.consumer_name = consumer_name
        if predicate:
            self.predicate = predicate
        self.batch_info = None
        assert self.queue_name
        assert self.consumer_name

    def __str__(self):
        return self.consumer_name

    @property
    def queue(self):
        if self._queue is None:
            self._queue = self._queue_class(self.queue_name)
        return self._queue

    def next_batches(self, curs, limit=None, commit=False):
        """Return all the pending batches.

        Iterate on the batches, and yield an iterator on the events.
        After events are processed close the batch, yield the next one.
        """
        n_batch = 0

        # Use a separate dict-like cursor
        with self._cursor(curs) as dict_cursor:
            while not limit or n_batch < limit:
                # acquire batch
                ev_list = self._load_next_batch(dict_cursor)
                if commit:
                    dict_cursor.connection.commit()

                if ev_list is None:
                    break
                n_batch += 1

                try:
                    # load and process events
                    yield ev_list
                except GeneratorExit:
                    if ev_list.fetch_status != 2:
                        # partially processed: do not 'finish_batch'
                        return
                    # all processed: break loop, but 'finish_batch' before
                    limit = n_batch

                # done
                ev_list.finish()
                if commit:
                    dict_cursor.connection.commit()

    def next_events(self, curs, limit=None, commit=False):
        """Return an iterator on the pending events.

        Iterate on the batches, and yield each event of each batch.
        After events are processed close the batch, yield the next one.
        """
        for ev_list in self.next_batches(curs, limit=limit, commit=commit):
            for ev in ev_list:
                yield ev

    def _cursor(self, session):
        """Return a separate cursor, sharing the same connection."""
        if hasattr(session, 'connection'):
            session = session.connection
        if session.autocommit and self.pgq_lazy_fetch:
            raise RuntimeError("autocommit mode is not compatible "
                               "with pgq_lazy_fetch")
        return session.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def _load_next_batch(self, curs):
        """Allocate next batch. (internal)"""
        q = "SELECT * FROM pgq.next_batch_custom(%s, %s, %s, %s, %s);"
        curs.execute(q, (self.queue_name, self.consumer_name, self.pgq_min_lag,
                         self.pgq_min_count, self.pgq_min_interval))
        inf = dict(curs.fetchone())
        inf['tick_id'] = inf['cur_tick_id']
        inf['batch_end'] = inf['cur_tick_time']
        inf['batch_start'] = inf['prev_tick_time']
        inf['seq_start'] = inf['prev_tick_event_seq']
        inf['seq_end'] = inf['cur_tick_event_seq']
        self.batch_info = inf
        batch_id = inf['batch_id']
        if batch_id is None:
            return batch_id
        return self._batch_class(curs, batch_id, self.queue_name,
                                 self.pgq_lazy_fetch, self.predicate)

    def register(self, curs, tick_id=None):
        """Register the consumer on the queue."""
        return self.queue.register_consumer(curs, self.consumer_name, tick_id)

    def unregister(self, curs):
        """Unregister the consumer from the queue."""
        return self.queue.unregister_consumer(curs, self.consumer_name)

    def get_info(self, curs):
        """Get info about the queue."""
        return self.queue.get_consumer_info(curs, self.consumer_name)


class _Connection(object):
    """Create a new database connection.

    Default connect_timeout is 15, unless it's set in the dsn.
    """

    def __init__(self, dsn, autocommit=False, appname=__file__):
        # allow override
        if 'connect_timeout' not in dsn:
            dsn += " connect_timeout=15"
        if 'application_name' not in dsn:
            dsn += " application_name=%s" % quote_dsn_param(appname)
        self._autocommit = autocommit
        self._connection = None
        self._dsn = dsn

    def cursor(self, cursor_factory=psycopg2.extras.DictCursor):
        connection = self._connection
        if not connection or connection.closed:
            connection = psycopg2.connect(self._dsn)
            connection.autocommit = self._autocommit
            self._connection = connection
        return connection.cursor(cursor_factory=cursor_factory)


class Ticker(_Connection):
    """PgQ ticker daemon."""
    _logger = logging.getLogger('PgQ Ticker')

    def __init__(self, dsn, config=None):
        if config is None:
            config = {}
        super(Ticker, self).__init__(dsn, autocommit=1, appname='PgQ Ticker')
        self.check_period = config.get('check_period', 60)
        self.maint_period = config.get('maint_period', 120)
        self.retry_period = config.get('retry_period', 30)
        self.stats_period = config.get('stats_period', 30)
        self.ticker_period = config.get('ticker_period', 1)
        self._next_ticker = self._next_maint = self._next_retry = 0
        self._next_stats = 0 if (self.stats_period and
                                 self.stats_period > 0) else 0x3afff43370
        self.n_ticks = self.n_maint = self.n_retry = 0

    def run(self):
        while True:
            self._logger.info("Starting Ticker %s", __version__)
            try:
                with self.cursor() as curs:
                    if not self.try_lock(curs):
                        self._logger.warning('Aborting.')
                        return
                    while True:
                        self.run_once(curs)
                        next_time = min(self._next_ticker,
                                        self._next_maint,
                                        self._next_retry)
                        time.sleep(max(1, next_time - time.time()))
            except Exception as exc:
                # Something bad happened, re-check the connection
                self._logger.warning("%s: %s", type(exc).__name__, exc)
                curs = self._connection = None
            time.sleep(self.check_period)

    def check_pgq(self, curs):
        curs.execute("SELECT 1 FROM pg_catalog.pg_namespace"
                     " WHERE nspname = 'pgq';")
        (res,) = curs.fetchone()
        if not res:
            self._logger.warning('no pgq installed')
            return False
        version = Queue.version(curs)
        if version < "3":
            self._logger.warning('bad pgq version: %s', version)
            return False
        return True

    def try_lock(self, curs):
        """Avoid running twice on the same database."""
        if not self.check_pgq(curs):
            return False
        curs.execute(
            "SELECT pg_try_advisory_lock(catalog.oid::int, pgq.oid::int)"
            "  FROM pg_database catalog, pg_namespace pgq"
            " WHERE datname=current_catalog AND nspname='pgq';")
        (res,) = curs.fetchone()
        if not res:
            self._logger.warning('already running')
            return False
        return True

    def run_once(self, curs):
        self.do_ticker(curs)

        if time.time() > self._next_maint:
            self.run_maint(curs)
            self._next_maint = time.time() + self.maint_period

        if time.time() > self._next_retry:
            self.run_retry(curs)
            self._next_retry = time.time() + self.retry_period

        if time.time() > self._next_stats:
            self.log_stats()
            self._next_stats = time.time() + self.stats_period

    def do_ticker(self, curs):
        if time.time() > self._next_ticker:
            curs.execute("SELECT pgq.ticker();")
            (res,) = curs.fetchone()
            self.n_ticks += 1
            self._next_ticker = time.time() + self.ticker_period
            return res

    def run_maint(self, curs):
        self._logger.debug("starting maintenance")
        curs.execute("SELECT func_name, func_arg FROM pgq.maint_operations();")
        for func_name, func_arg in curs.fetchall():
            if func_name.lower().startswith('vacuum'):
                assert func_arg
                statement = "%s %s;" % (func_name, quote_ident(func_arg))
                params = None
            elif func_arg:
                statement = "SELECT %s(%%s);" % func_name
                params = (func_arg,)
            else:
                statement = "SELECT %s();" % func_name
                params = None
            self._logger.debug("[%s]", statement)
            curs.execute(statement, params)
            self.n_maint += 1
            self.do_ticker(curs)

    def run_retry(self, curs):
        self._logger.debug("starting retry event processing")
        retry = True
        while retry:
            curs.execute("SELECT * FROM pgq.maint_retry_events();")
            (retry,) = curs.fetchone()
            self.n_retry += retry
            self.do_ticker(curs)

    def log_stats(self):
        self._logger.info(str(self))

    def __str__(self):
        return ("{ticks: %(n_ticks)d, maint: %(n_maint)d,"
                " retry: %(n_retry)d}" % self.__dict__)


def _main(log_level=logging.INFO):
    import getpass
    import sys
    import threading

    if len(sys.argv) < 2 or '=' not in sys.argv[1]:
        print("""PgQ Ticker daemon.

Usage:
  %s DSN

  If the password is missing, it will be requested interactively.
""" % sys.argv[0])
        sys.exit(1)

    dsn = ' '.join(sys.argv[1:])
    if 'password' not in dsn:
        passwd = getpass.getpass("Password: ")
        if passwd is not None:
            dsn += " password=%s" % quote_dsn_param(passwd)

    # Configure the logger
    logging.basicConfig(level=log_level)

    ticker = Ticker(dsn)
    # ticker._logger.setLevel(logging.DEBUG)

    if sys.flags.interactive:
        t = threading.Thread(target=ticker.run)
        t.daemon = True
        t.start()
    else:
        try:
            ticker.run()
        except KeyboardInterrupt:
            print('\nstopped ...')

    return ticker


if __name__ == '__main__':
    ticker = _main()
