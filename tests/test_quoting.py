# -*- coding: utf-8 -*-

from decimal import Decimal

import unittest2
import pgqueue


class TestQuoting(unittest2.TestCase):

    def test_quote_ident(self):
        # Used for SQL identifiers
        quote_ident = pgqueue.quote_ident
        self.assertEqual(quote_ident(''), '""')
        self.assertEqual(quote_ident('any_table'), '"any_table"')
        self.assertEqual(quote_ident('"other"_table'), '"""other""_table"')

    def test_quote_copy(self):
        # Used for passing values to COPY FROM command
        quote_copy = pgqueue.quote_copy
        self.assertEqual(quote_copy(None), r'\N')
        self.assertEqual(quote_copy(''), '')
        self.assertEqual(quote_copy(1.0), '1.0')
        self.assertEqual(quote_copy(True), 'True')
        self.assertEqual(quote_copy(Decimal("1")), '1')
        self.assertEqual(quote_copy('any value'), 'any value')
        self.assertEqual(quote_copy('any\tvalue'), r'any\tvalue')
        self.assertEqual(quote_copy('any\\tvalue'), r'any\\tvalue')
        self.assertEqual(quote_copy('a\r\nlong\ntext'), r'a\r\nlong\ntext')

    def test_quote_dsn_param(self):
        # Used for quoting dsn password, or other dsn parameters
        param = pgqueue.quote_dsn_param
        self.assertEqual(param(None), "''")
        self.assertEqual(param(""), "''")
        self.assertEqual(param("1password"), "1password")
        self.assertEqual(param(" p ssw rd"), "' p ssw rd'")
        self.assertEqual(param(r"p'ssw\rd"), r"'p\'ssw\\rd'")
