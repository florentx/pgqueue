# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import mock
import unittest2

import pgqueue

BATCH_INFO = {
    'batch_id': 42,
    'cur_tick_id': 0xabc,
    'cur_tick_time': 0xba9,
    'cur_tick_event_seq': 0x7a,
    'prev_tick_time': 0xba1,
    'prev_tick_event_seq': 0x79,
}
BATCH_NULL = dict.fromkeys(BATCH_INFO)

EVENT0 = dict.fromkeys(('ev_id', 'ev_time', 'ev_txid', 'ev_retry',
                        'ev_type', 'ev_data',
                        'ev_extra1', 'ev_extra2', 'ev_extra3', 'ev_extra4'))
EVENT1 = dict(EVENT0, ev_id=94768, ev_txid=2133514,
              ev_time=datetime.now() - timedelta(minutes=7),
              ev_type='NOTE', ev_data='the payload', ev_extra4='42')
EVENT2 = dict(EVENT0, ev_id=94769)
EVENT3 = dict(EVENT0, ev_id=94770)
EVENT4 = dict(EVENT0, ev_id=94771)

NEXT_BATCH = 'SELECT * FROM pgq.next_batch_custom(%s, %s, %s, %s, %s);'
BATCH_CURS = 'SELECT * FROM pgq.get_batch_cursor(%s, %s, %s, %s);'
ANY = mock.ANY
C = mock.call
# Should be C.__iter__(), but it is not supported by mock
C__iter__ = ('__iter__', ())


def mock_cursor(autocommit=False):
    """Return a mock cursor."""
    cursor = mock.MagicMock()
    cursor.connection.autocommit = autocommit
    # return the same cursor when used as a context manager
    cursor.connection.cursor.return_value.__enter__.return_value = cursor

    # http://bugs.python.org/issue18622
    def safe_reset_mock():
        cursor.connection.cursor.return_value.__enter__.return_value = None
        cursor.reset_mock()
        cursor.connection.cursor.return_value.__enter__.return_value = cursor
    cursor.safe_reset_mock = safe_reset_mock
    return cursor


class TestConsumer(unittest2.TestCase):

    def test_register(self):
        cur = mock_cursor(autocommit=True)

        consu = pgqueue.Consumer('main_q', 'first')
        self.assertEqual(consu.queue_name, 'main_q')
        self.assertEqual(consu.consumer_name, 'first')
        self.assertIsNone(consu.predicate)
        self.assertIsInstance(consu.queue, pgqueue.Queue)
        self.assertEqual(consu.queue.queue_name, 'main_q')

        # new consumer
        consu.register(cur)

        # retrieve information
        consu.get_info(cur)

        # remove consumer
        consu.unregister(cur)

        self.assertSequenceEqual(cur.execute.call_args_list, [
            C('SELECT pgq.register_consumer_at(%s, %s, %s);',
              ('main_q', 'first', None)),
            C('SELECT * FROM pgq.get_consumer_info(%s, %s);',
              ('main_q', 'first')),
            C('SELECT pgq.unregister_consumer(%s, %s);', ('main_q', 'first')),
        ])

    def test_next_batches(self):
        cur = mock_cursor()
        cur.fetchone.side_effect = [BATCH_INFO, BATCH_NULL]
        consu = pgqueue.Consumer('main_q', 'first')

        for batch in consu.next_batches(cur, limit=1, commit=True):
            self.assertIsInstance(batch, pgqueue.Batch)

        self.assertSequenceEqual(cur.execute.call_args_list, [
            C(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C('SELECT pgq.finish_batch(%s);', (42,)),
        ])

    def test_next_events(self):
        cur = mock_cursor()
        cur.fetchone.side_effect = [BATCH_INFO, BATCH_NULL]
        cur.rowcount = 42
        consu = pgqueue.Consumer('main_q', 'first')

        for event in consu.next_events(cur, limit=1, commit=True):
            self.assertIsInstance(event, pgqueue.Event)

        self.assertSequenceEqual(cur.execute.call_args_list, [
            C(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C(BATCH_CURS, (42, 'batch_walker', 300, None)),
            C('CLOSE batch_walker;'),
            C('SELECT pgq.finish_batch(%s);', (42,)),
        ])


class TestEvent(unittest2.TestCase):
    maxDiff = 0x800

    def test_simple(self):
        cur = mock_cursor()
        cur.fetchone.side_effect = [BATCH_INFO, BATCH_NULL]
        cur.__iter__.side_effect = [iter([EVENT1, EVENT2])]
        cur.rowcount = 2
        consu = pgqueue.Consumer('main_q', 'first')

        events = []
        for event in consu.next_events(cur, limit=1, commit=True):
            self.assertIsInstance(event, pgqueue.Event)
            events.append(str(event))

            self.assertFalse(event.failed)
            self.assertIsNone(event.retry_time)
            self.assertEqual(event.retry, 0)
            # 11 attributes on the Event object (including _failed mapping)
            self.assertEqual(len(event._fields), 11)
            # the main interface is the namedtuple API
            for idx, name in enumerate(event._fields):
                self.assertIs(getattr(event, name), event[idx])
            # use either vars(event) or event.__dict__ when you need a dict
            self.assertEqual(sorted(event.__dict__), sorted(event._fields))
            self.assertEqual(vars(event), event.__dict__)

        self.assertEqual(events, [
            '<id=94768 type=NOTE data=the payload'
            ' e1=None e2=None e3=None e4=42>',
            '<id=94769 type=None data=None e1=None e2=None e3=None e4=None>',
        ])

        self.assertSequenceEqual(cur.mock_calls, [
            C.connection.cursor(cursor_factory=ANY),
            C.connection.cursor().__enter__(),
            C.execute(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C.fetchone(),
            C.connection.commit(),
            C.execute(BATCH_CURS, (42, 'batch_walker', 300, None)),
            C__iter__,
            C.execute('CLOSE batch_walker;'),
            C.execute('SELECT pgq.finish_batch(%s);', (42,)),
            C.connection.commit(),
            C.connection.cursor().__exit__(None, None, None),
        ])

    def test_retry(self):
        cur = mock_cursor()
        cur.fetchone.side_effect = [BATCH_INFO, BATCH_NULL]
        cur.__iter__.side_effect = [iter([EVENT1, EVENT2])]
        cur.rowcount = 2
        consu = pgqueue.Consumer('main_q', 'first')

        for event in consu.next_events(cur, limit=1, commit=True):
            self.assertIsInstance(event, pgqueue.Event)

            # Tag for retry
            event.tag_retry(199)
            self.assertTrue(event.failed)
            self.assertEqual(event.retry_time, 199)
            # This is the incremental counter of retries, None initially
            self.assertEqual(event.retry, 0)

            # Tag is reversible, until the batch is "finished"
            event.tag_done()
            self.assertFalse(event.failed)
            self.assertIsNone(event.retry_time)
            self.assertEqual(event.retry, 0)

            event.tag_retry(99)
            self.assertTrue(event.failed)
            self.assertEqual(event.retry_time, 99)
            self.assertEqual(event.retry, 0)

        self.assertSequenceEqual(cur.mock_calls, [
            C.connection.cursor(cursor_factory=ANY),
            C.connection.cursor().__enter__(),
            C.execute(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C.fetchone(),
            C.connection.commit(),
            C.execute(BATCH_CURS, (42, 'batch_walker', 300, None)),
            C__iter__,
            C.execute('CLOSE batch_walker;'),
            # Argument is a generator
            C.executemany('SELECT pgq.event_retry(%s, %s, %s);', ANY),
            C.execute('SELECT pgq.finish_batch(%s);', (42,)),
            C.connection.commit(),
            C.connection.cursor().__exit__(None, None, None),
        ])

    def test_large_batch(self):
        cur = mock_cursor()
        cur.fetchone.side_effect = [BATCH_INFO, BATCH_NULL]

        def execute(command, *params, _rowcounts=[3, 1, 0]):
            if command == BATCH_CURS or command.startswith('FETCH'):
                cur.rowcount = _rowcounts.pop(0)
            else:
                cur.rowcount = mock.Mock()

        cur.execute.side_effect = execute
        cur.__iter__.side_effect = [iter([EVENT1, EVENT2, EVENT3]),
                                    iter([EVENT4])]
        consu = pgqueue.Consumer('main_q', 'first')
        consu.pgq_lazy_fetch = 3    # instead of 300

        events = []
        for event in consu.next_events(cur, commit=True):
            self.assertIsInstance(event, pgqueue.Event)
            events.append(str(event))

        self.assertEqual(events, [
            '<id=94768 type=NOTE data=the payload'
            ' e1=None e2=None e3=None e4=42>',
            '<id=94769 type=None data=None e1=None e2=None e3=None e4=None>',
            '<id=94770 type=None data=None e1=None e2=None e3=None e4=None>',
            '<id=94771 type=None data=None e1=None e2=None e3=None e4=None>',
        ])

        self.assertSequenceEqual(cur.mock_calls, [
            C.connection.cursor(cursor_factory=ANY),
            C.connection.cursor().__enter__(),
            C.execute(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C.fetchone(),
            C.connection.commit(),
            C.execute(BATCH_CURS, (42, 'batch_walker', 3, None)),
            C__iter__,
            C.execute('FETCH 3 FROM batch_walker;'),
            C__iter__,
            C.execute('CLOSE batch_walker;'),
            C.execute('SELECT pgq.finish_batch(%s);', (42,)),
            C.connection.commit(),
            C.execute(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C.fetchone(),
            C.connection.commit(),
            C.connection.cursor().__exit__(None, None, None),
        ])

    def test_abort(self):
        cur = mock_cursor()
        cur.fetchone.side_effect = [BATCH_INFO, BATCH_NULL]

        def execute(command, *params, _rowcounts=[3, 1, 0]):
            if command == BATCH_CURS or command.startswith('FETCH'):
                cur.rowcount = _rowcounts.pop(0)
            else:
                cur.rowcount = mock.Mock()

        cur.execute.side_effect = execute
        cur.__iter__.side_effect = [iter([EVENT1, EVENT2, EVENT3]),
                                    iter([EVENT4])]
        consu = pgqueue.Consumer('main_q', 'first')
        consu.pgq_lazy_fetch = 3    # instead of 300

        events = []
        for event in consu.next_events(cur, commit=True):
            events.append(str(event))
            if len(events) == 4:
                break

        self.assertSequenceEqual(cur.mock_calls, [
            C.connection.cursor(cursor_factory=ANY),
            C.connection.cursor().__enter__(),
            C.execute(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C.fetchone(),
            C.connection.commit(),
            C.execute(BATCH_CURS, (42, 'batch_walker', 3, None)),
            C__iter__,
            C.execute('FETCH 3 FROM batch_walker;'),
            C__iter__,
            # the transaction is not committed: implicit rollback
            C.connection.cursor().__exit__(None, None, None),
        ])


class TestBatch(unittest2.TestCase):

    def test_simple(self):
        cur = mock_cursor()
        cur.fetchone.side_effect = [BATCH_INFO, BATCH_NULL]
        consu = pgqueue.Consumer('main_q', 'first')

        for batch in consu.next_batches(cur, commit=True):
            self.assertIsInstance(batch, pgqueue.Batch)
            self.assertEqual(str(batch), '<Batch main_q:42>')

        self.assertSequenceEqual(cur.mock_calls, [
            C.connection.cursor(cursor_factory=ANY),
            C.connection.cursor().__enter__(),
            C.execute(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C.fetchone(),
            C.connection.commit(),
            C.execute('SELECT pgq.finish_batch(%s);', (42,)),
            C.connection.commit(),
            C.execute(NEXT_BATCH, ('main_q', 'first', None, None, None)),
            C.fetchone(),
            C.connection.commit(),
            C.connection.cursor().__exit__(None, None, None),
        ])
