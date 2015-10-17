# -*- coding: utf-8 -*-
import sys

from setuptools import setup


def get_version(fname='pgqueue.py'):
    with open(fname) as f:
        for line in f:
            if line.startswith('__version__'):
                return eval(line.split('=')[-1])


def get_long_description():
    descr = []
    for fname in ('README.rst',):
        with open(fname) as f:
            descr.append(f.read())
    return '\n\n'.join(descr)


if sys.version_info < (3,):
    tests_require = ['mock', 'unittest2'],
    test_suite = 'unittest2.collector'
else:
    tests_require = ['mock', 'unittest2py3k']
    test_suite = 'unittest2.collector.collector'


setup(
    name="pgqueue",
    license="ISC",
    version=get_version(),
    description="Light PgQ Framework - queuing system for PostgreSQL",
    long_description=get_long_description(),
    maintainer="Florent Xicluna",
    maintainer_email="florent.xicluna@gmail.com",
    url="https://github.com/florentx/pgqueue",
    py_modules=['pgqueue'],
    install_requires=[
        'psycopg2',
    ],
    zip_safe=False,
    keywords="postgresql pgq queue",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    tests_require=tests_require,
    test_suite=test_suite,
)
