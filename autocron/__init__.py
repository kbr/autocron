"""
autocron:

simple asynchronous background task handling with no dependencies beside
the standard-library.
"""

# this module is part of autocron
# copyright (c) 2024 Klaus Bremer
# all rights reserved
#
# license: MIT

from autocron.decorators import (
    cron,
    delay,
)
from autocron.engine import Engine
from autocron.sqlite_interface import SQLiteInterface


__all__ = ["cron", "delay", "start", "stop"]
__version__ = "1.2.1"

_engine = Engine()
_interface = SQLiteInterface()


def start(database_file, workers=None):
    """
    Call this from the framework of choice to explicitly activate
    autocron. ``database_file`` is a string with the file-name of the
    database. The file gets stored in the ``~.autocron/`` directory. If
    this directory does not exist, it will get created. The file-name can
    also be an absolute path so the file will get stored elsewere. In
    this case all directories in the path must exist.

    With ``workers`` the number of worker is set and stored in the
    database. If the value is ``None`` (default) the number of workers
    are read from the database.
    """
    _engine.start(database_file=database_file, workers=workers)


def stop():
    """
    Stops autocron explicitly. On receiving a termination-signal
    autocron invokes a shutdown sequence to stop the workers, so calling
    ``stop`` is normalwise not required.
    """
    _engine.stop()
