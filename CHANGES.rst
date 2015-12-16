Changelog
=========


0.5 (2015-12-16)
~~~~~~~~~~~~~~~~

* Log the reconnections.

* Single function call to retry any number of events.

* Lower memory usage.


0.4.1 (2015-10-17)
~~~~~~~~~~~~~~~~~~

* Fix a bug with PgQ Ticker after January 18th, 2038.
  Now it's safe until December 31st, 9999.


0.4 (2014-09-22)
~~~~~~~~~~~~~~~~

* Ensure ``Event.retry`` is numeric, never ``None``.

* Reset the PgQ Ticker connection after a database outage.


0.3 (2014-05-25)
~~~~~~~~~~~~~~~~

* First public release
