"""
autocron:

simple asynchronous background task handling with no dependencies beside
the standard-library.
"""
from .decorators import (
    cron,
    delay,
)
from .engine import Engine
from .sql_interface import SQLiteInterface


__all__ = ["cron", "delay", "start", "stop", "get_results"]
__version__ = "0.9.0"

_engine = Engine()
_interface = SQLiteInterface()


def start(database_file):
    """
    Call this from the framework of choice to explicitly activate
    autocron. ``database_file`` is a string with the file-name of the
    database. The file gets stored in the ``~.autocron/`` directory. If
    this directory does not exist, it will get created. The file-name can
    also be an absolute path so the file will get stored elsewere. In
    this case all directories in the path must exist.
    """
    _engine.start(database_file=database_file)


def stop():
    """
    Stops autocron explicitly. On receiving a termination-signal
    autocron invokes a shutdown sequence to stop the workers, so calling
    ``stop`` is normalwise not required.
    """
    _engine.stop()


def get_results():
    """
    Returns a list with all result entries from delayed functions.
    """
    return _interface.get_results()
