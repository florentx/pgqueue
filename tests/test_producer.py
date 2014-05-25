# -*- coding: utf-8 -*-

import mock
import unittest2

import pgqueue


class TestProducer(unittest2.TestCase):

    def test_insert_event(self):
        cur = mock.MagicMock()

        pgqueue.insert_event(cur, 'main_q', 'NOTE', 'Hello world!')

        self.assertSequenceEqual(cur.execute.call_args_list, [
            mock.call(
                'SELECT pgq.insert_event(%s, %s, %s, %s, %s, %s, %s);',
                ('main_q', 'NOTE', 'Hello world!', None, None, None, None)),
        ])
        cur.reset_mock()

        pgqueue.insert_event(cur, 'main_q', 'NOTE', 'Hello!',
                             extra1='42', extra4='123"ac')

        self.assertSequenceEqual(cur.execute.call_args_list, [
            mock.call(
                'SELECT pgq.insert_event(%s, %s, %s, %s, %s, %s, %s);',
                ('main_q', 'NOTE', 'Hello!', '42', None, None, '123"ac')),
        ])

    def test_bulk_insert_events(self):
        cur = mock.MagicMock()
        cur.fetchone.return_value = ('pgq.event_1_2',)
        rows = [('ab', '12'), (None, None), ('', '-3'), ('tab\t.', '42\r\n')]
        columns = ('data', 'extra3')

        pgqueue.bulk_insert_events(cur, 'main_q', rows, columns)

        # Fake file object is a subclass of dict
        fobj = {'content': 'ab\t12\n'
                           '\\N\t\\N\n'
                           '\t-3\n'
                           'tab\\t.\t42\\r\\n\n'}
        self.assertSequenceEqual(cur.execute.call_args_list, [
            mock.call('SELECT pgq.current_event_table(%s);', ('main_q',)),
        ])
        self.assertSequenceEqual(cur.copy_from.call_args_list, [
            mock.call(fobj, 'pgq.event_1_2', columns=['ev_data', 'ev_extra3']),
        ])

    def test_queue_insert_event(self):
        cur = mock.MagicMock()

        queue = pgqueue.Queue('main_q')
        queue.insert_event(cur, 'NOTE', 'Hello world!')

        self.assertSequenceEqual(cur.execute.call_args_list, [
            mock.call(
                'SELECT pgq.insert_event(%s, %s, %s, %s, %s, %s, %s);',
                ('main_q', 'NOTE', 'Hello world!', None, None, None, None)),
        ])
        cur.reset_mock()

        queue.insert_event(cur, 'NOTE', 'Hello!', extra1='42', extra4='123"ac')

        self.assertSequenceEqual(cur.execute.call_args_list, [
            mock.call(
                'SELECT pgq.insert_event(%s, %s, %s, %s, %s, %s, %s);',
                ('main_q', 'NOTE', 'Hello!', '42', None, None, '123"ac')),
        ])

    def test_queue_insert_events(self):
        cur = mock.MagicMock()
        cur.fetchone.return_value = ('pgq.event_1_2',)
        rows = [('ab', '12'), (None, None), ('', '-3'), ('tab\t.', '42\r\n')]
        columns = ('data', 'extra3')

        queue = pgqueue.Queue('main_q')
        queue.insert_events(cur, rows, columns)

        # Fake file object is a subclass of dict
        fobj = {'content': 'ab\t12\n'
                           '\\N\t\\N\n'
                           '\t-3\n'
                           'tab\\t.\t42\\r\\n\n'}
        self.assertSequenceEqual(cur.execute.call_args_list, [
            mock.call('SELECT pgq.current_event_table(%s);', ('main_q',)),
        ])
        self.assertSequenceEqual(cur.copy_from.call_args_list, [
            mock.call(fobj, 'pgq.event_1_2', columns=['ev_data', 'ev_extra3']),
        ])


class TestQueue(unittest2.TestCase):
    maxDiff = 0x800

    def test_create_queue(self):
        cur = mock.MagicMock()

        queue = pgqueue.Queue('main_queue')
        queue.create(cur)
        queue.drop(cur)

        self.assertSequenceEqual(cur.execute.call_args_list, [
            mock.call('SELECT pgq.create_queue(%s);', ('main_queue',)),
            mock.call('SELECT pgq.drop_queue(%s, %s);', ('main_queue', False)),
        ])
        cur.reset_mock()

        queue = pgqueue.Queue('main_queue')
        queue.create(cur, ticker_max_count=500, ticker_max_lag='3 seconds',
                     ticker_idle_period='1 minute', rotation_period='2 hours')
        queue.register_consumer(cur, 'first_consumer')
        queue.drop(cur)
        queue.drop(cur, force=True)

        # re-order the call_args_list to verify assertions
        execute_args = list(cur.execute.call_args_list)
        execute_args[1:5] = sorted(execute_args[1:5])
        qset = 'SELECT pgq.set_queue_config(%s, %s, %s);'
        self.assertSequenceEqual(execute_args, [
            mock.call('SELECT pgq.create_queue(%s);', ('main_queue',)),
            mock.call(qset, ('main_queue', 'rotation_period', '2 hours')),
            mock.call(qset, ('main_queue', 'ticker_idle_period', '1 minute')),
            mock.call(qset, ('main_queue', 'ticker_max_count', '500')),
            mock.call(qset, ('main_queue', 'ticker_max_lag', '3 seconds')),
            mock.call('SELECT pgq.register_consumer_at(%s, %s, %s);',
                      ('main_queue', 'first_consumer', None)),
            mock.call('SELECT pgq.drop_queue(%s, %s);', ('main_queue', False)),
            mock.call('SELECT pgq.drop_queue(%s, %s);', ('main_queue', True)),
        ])

    def test_api(self):
        cur = mock.MagicMock()

        queue = pgqueue.Queue('main_queue')
        self.assertEqual(queue.queue_name, 'main_queue')

        # Configure queue
        queue.create(cur)
        queue.set_config(cur, 'ticker_paused', True)
        queue.register_consumer(cur, 'first_consumer')
        queue.unregister_consumer(cur, 'first_consumer')

        # Retrieve queue information
        queue.get_info(cur)
        queue.get_consumer_info(cur)
        queue.get_consumer_info(cur, 'first_consumer')

        # Global information
        queue.version(cur)
        queue.get_all_queues_info(cur)
        queue.get_all_consumers_info(cur)

        self.assertSequenceEqual(cur.execute.call_args_list, [
            # configuration
            mock.call('SELECT pgq.create_queue(%s);', ('main_queue',)),
            mock.call('SELECT pgq.set_queue_config(%s, %s, %s);',
                      ('main_queue', 'ticker_paused', 'True')),
            mock.call('SELECT pgq.register_consumer_at(%s, %s, %s);',
                      ('main_queue', 'first_consumer', None)),
            mock.call('SELECT pgq.unregister_consumer(%s, %s);',
                      ('main_queue', 'first_consumer')),
            # queue information
            mock.call('SELECT * FROM pgq.get_queue_info(%s);',
                      ('main_queue',)),
            mock.call('SELECT * FROM pgq.get_consumer_info(%s, %s);',
                      ('main_queue', None)),
            mock.call('SELECT * FROM pgq.get_consumer_info(%s, %s);',
                      ('main_queue', 'first_consumer')),
            # global information
            mock.call('SELECT pgq.version();'),
            mock.call('SELECT * FROM pgq.get_queue_info();'),
            mock.call('SELECT * FROM pgq.get_consumer_info();'),
        ])
