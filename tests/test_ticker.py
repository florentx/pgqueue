# -*- coding: utf-8 -*-

import unittest2
import mock

import pgqueue

DSN = "dbname=test_db user=postgres"
C = mock.call


class TestTicker(unittest2.TestCase):
    maxDiff = 0x800

    def setUp(self):
        self.pg_connect = mock.patch('pgqueue.psycopg2.connect').start()
        self.pg_cursor = mock.MagicMock()
        (self.pg_connect.return_value
             .cursor.return_value.__enter__.return_value) = self.pg_cursor

    def tearDown(self):
        mock.patch.stopall()
        del self.pg_connect, self.pg_cursor

    def test_create(self):
        ticker = pgqueue.Ticker(DSN)
        self.assertEqual(ticker.check_period, 60)
        self.assertEqual(ticker.maint_period, 120)
        self.assertEqual(ticker.retry_period, 30)
        self.assertEqual(ticker.stats_period, 30)
        self.assertEqual(ticker.ticker_period, 1)
        self.assertEqual(ticker.check_period, 60)

    def test_run(self):
        ticker = pgqueue.Ticker(DSN)
        maint_operations = [
            ('pgq.maint_rotate_tables_step1', 'main_queue'),
            ('vacuum', 'pgq.dummy'),
        ]
        self.pg_cursor.fetchone.side_effect = [
            (1,), ('3.1.5',), (42,),    # pgq_check() and try_lock()
            (42,), (42,), (0,),         # 1st run
            (42,), (0,),                # 2nd run
            (42,)]                      # 3rd run
        self.pg_cursor.fetchall.return_value = maint_operations

        def force_run():
            ticker._next_ticker = 0x411
            ticker._next_retry = 0x421
            ticker._next_maint = 0x431
            yield
            ticker._next_ticker = 0x511
            yield
            ticker._next_ticker = 0x611
            raise SystemExit
        with mock.patch('pgqueue.time.sleep', side_effect=force_run()):
            self.assertRaises(SystemExit, ticker.run)
        self.assertEqual(str(ticker), '{ticks: 3, maint: 4, retry: 42}')

        self.assertSequenceEqual(self.pg_connect.call_args_list, [
            C("dbname=test_db user=postgres connect_timeout=15 "
              "application_name='PgQ Ticker'"),
        ])
        self.assertSequenceEqual(self.pg_cursor.execute.call_args_list, [
            C("SELECT 1 FROM pg_catalog.pg_namespace"
              " WHERE nspname = 'pgq';"),
            C("SELECT pgq.version();"),
            C("SELECT pg_try_advisory_lock(catalog.oid::int, pgq.oid::int)"
              "  FROM pg_database catalog, pg_namespace pgq"
              " WHERE datname=current_catalog AND nspname='pgq';"),
            # 1st run
            C("SELECT pgq.ticker();"),
            C('SELECT func_name, func_arg FROM pgq.maint_operations();'),
            C('SELECT pgq.maint_rotate_tables_step1(%s);', ('main_queue',)),
            C('vacuum "pgq.dummy";', None),
            C('SELECT * FROM pgq.maint_retry_events();'),
            C('SELECT * FROM pgq.maint_retry_events();'),
            # 2nd run
            C("SELECT pgq.ticker();"),
            C('SELECT func_name, func_arg FROM pgq.maint_operations();'),
            C('SELECT pgq.maint_rotate_tables_step1(%s);', ('main_queue',)),
            C('vacuum "pgq.dummy";', None),
            C('SELECT * FROM pgq.maint_retry_events();'),
            # 3rd run
            C("SELECT pgq.ticker();"),
        ])
