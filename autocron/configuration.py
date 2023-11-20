"""
configuration.py

Module to get the configuration from the standard (default) settings or
adapt them from web-frameworks (currently django).
"""

import configparser
import datetime
import pathlib

try:
    from django.conf import settings
except ImportError:
    DJANGO_IS_INSTALLED = False
else:
    DJANGO_IS_INSTALLED = True

AUTOCRON_DIRECTORY = ".autocron"
DB_FILE_NAME = "autocron.db"
DEFAULT_PROJECT_NAME = "autocron_storage"

SEMAPHORE_FILE_NAME = "autocron.semaphore"
CONFIGURATION_FILE_NAME = "autocron.conf"
CONFIGURATION_SECTION = "autocron"
MONITOR_IDLE_TIME = 2.0  # seconds
WORKER_IDLE_TIME = 4.0  # seconds
RESULT_TTL = 1800  # storage time (time to live) for results in seconds
CONFIGURABLE_SETTING_NAMES = (
    "monitor_idle_time",
    "worker_idle_time",
    "result_ttl",
)


class Configuration:
    """
    Class providing the configuration settings.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, project_name=DEFAULT_PROJECT_NAME, db_filename=DB_FILE_NAME):
        self.project_name = project_name
        self.db_filename = db_filename
        self.monitor_idle_time = MONITOR_IDLE_TIME
        self.worker_idle_time = WORKER_IDLE_TIME
        self.result_ttl = datetime.timedelta(minutes=RESULT_TTL)
        self._autocron_path = None
        self.is_active = True
#         self._read_configuration()

    @property
    def autocron_path(self):
        if self._autocron_path is None:
            try:
                home_dir = pathlib.Path().home()
            except RuntimeError:
                # can't resolve homedir, take the present working
                # directory. Depending on the application .gitignore
                # should get extended with a ".autocron/*" entry.
                home_dir = pathlib.Path.cwd()
            storage = home_dir / AUTOCRON_DIRECTORY / self.project_name
            storage.mkdir(parents=True, exist_ok=True)
            self._autocron_path = storage
        return self._autocron_path

    def _get_autocron_directory(self):
        """
        Return the directory as a Path object, where the autocron files
        are stored. These files are the database, the semaphore file and
        an optional configuration files. This directory is typically

            "~.autocron/project_name/"

        """
        try:
            home_dir = pathlib.Path().home()
        except RuntimeError:
            # can't resolve homedir, take the present working
            # directory. Depending on the application .gitignore
            # should get extended with a ".autocron/*" entry.
            home_dir = self.cwd
            prefix = None
        else:
            prefix = self.cwd.as_posix().replace("/", "_")
        autocron_dir = home_dir / ".autocron"
        if prefix:
            autocron_dir = autocron_dir / prefix
        autocron_dir.mkdir(parents=True, exist_ok=True)
        return autocron_dir

    def _read_configuration(self):
        """
        Read configuration data from an optional configuration file.
        The file must be in the autocron-directory named "autocron.conf".
        """
        parser = configparser.ConfigParser()
        if parser.read(self.configuration_file):
            # success
            try:
                values = parser[CONFIGURATION_SECTION]
            except KeyError:
                # ignore misconfigured file
                pass
            else:
                for name in CONFIGURABLE_SETTING_NAMES:
                    value = values.getfloat(name)
                    if value is not None:
                        self.__dict__[name] = value
                try:
                    value = values.getboolean("is_active")
                    if value is not None:
                        setattr(self, "is_active", value)
#                     setattr(self, "is_active", values.getboolean("is_active"))
                except ValueError:
                    pass

    def get_django_debug_setting(self):
        """
        Returns a boolean representing the django DEBUG setting.
        Returns None in case django is installed but there is an error
        reading the DEBUG setting.
        Raise a NameError in case this method is called but django is
        not installed.
        """
        return settings.DEBUG

    @property
    def configuration_file(self):
        """
        Provides the path to the configuration-file.
        """
        return self.autocron_path / CONFIGURATION_FILE_NAME

    @property
    def db_file(self):
        """
        Provides the path to the database-file.
        """
        return self.autocron_path / self.db_filename

    @property
    def semaphore_file(self):
        """
        Provides the path to the semaphore-file.
        """
        return self.autocron_path / SEMAPHORE_FILE_NAME

    @property
    def cwd(self):
        """
        Provides the current working directory.
        This is the working directory of the autocron importing application.
        """
        return pathlib.Path.cwd()

    @property
    def is_django_application(self):
        """
        Assume that if django is installed it is a django-application.
        Provides a boolean.
        """
        return DJANGO_IS_INSTALLED


configuration = Configuration()
