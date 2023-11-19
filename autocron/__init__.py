"""
autocron:

simple background task handling with no dependencies.
"""
from . import configuration
from .decorators import (
    cron,
    delay,
)
from .engine import engine as _engine


__all__ = ["cron", "delay", "django_autostart", "start"]
__version__ = "0.4.dev"


def start(database_file=None):
    """
    Call this from the framework of choice to explicitly
    activate autocron.
    """
    _engine.start(database_file=database_file)


def django_autostart(database_file=None):
    """
    Start autocron on a django-application depending on the
    debug-settings. If debug is True, autocron will not start.
    """
    debug = configuration.configuration.get_django_debug_setting()
    if not debug:
        start(database_file=database_file)
