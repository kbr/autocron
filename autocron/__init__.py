"""
autocron:

simple background task handling with no dependencies.
"""
from .decorators import (
    cron,
    delay,
)
from .engine import engine as _engine


__all__ = ["cron", "delay", "start"]
__version__ = "0.4.dev"


def start(database_file=None):
    """
    Call this from the framework of choice to explicitly
    activate autocron.
    """
    _engine.start(database_file=database_file)
