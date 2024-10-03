"""
The sqlite interface.

Data models, sql-definition and access routines.
"""

# this module is part of autocron
# copyright (c) 2024 Klaus Bremer
# all rights reserved
#
# license: MIT

# some structures are larger than pylint likes:
# pylint: disable=too-many-lines
# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes

import datetime
import functools
import pathlib
import pickle
import queue
import signal
import sqlite3
import threading
import time
import uuid


DEFAULT_STORAGE = ".autocron"
TEMPORARY_PREFIX = ".temp-"
REGISTER_BACKGROUND_TASK_TIMEOUT = 2.0

SQLITE_OPERATIONAL_ERROR_RETRIES = 100
SQLITE_OPERATIONAL_ERROR_DELAY = 0.01
SQLITE_DELAY_INCREMENT_STEPS = 20
SQLITE_DELAY_INCREMENT_FACTOR = 1.5

SQLITE_EXCLUSIVE_ACCESS = "BEGIN EXCLUSIVE"

SETTINGS_DEFAULT_WORKERS = 1
SETTINGS_DEFAULT_RUNNING_WORKERS = 0
SETTINGS_DEFAULT_MONITOR_LOCK = False
SETTINGS_DEFAULT_AUTOCRON_LOCK = False
SETTINGS_DEFAULT_MONITOR_IDLE_TIME = 5  # seconds
SETTINGS_DEFAULT_WORKER_IDLE_TIME = 0  # 0 seconds means auto idle time
SETTINGS_DEFAULT_WORKER_PIDS = ""
SETTINGS_DEFAULT_RESULT_TTL = 1800  # Storage time (time to live) in seconds
SETTINGS_DEFAULT_BLOCKING_MODE = False

SETTINGS_DEFAULT_DATA = {
    "max_workers": SETTINGS_DEFAULT_WORKERS,
    "running_workers": SETTINGS_DEFAULT_RUNNING_WORKERS,
    "monitor_lock": SETTINGS_DEFAULT_MONITOR_LOCK,
    "autocron_lock": SETTINGS_DEFAULT_AUTOCRON_LOCK,
    "monitor_idle_time": SETTINGS_DEFAULT_MONITOR_IDLE_TIME,
    "worker_idle_time": SETTINGS_DEFAULT_WORKER_IDLE_TIME,
    "worker_pids": SETTINGS_DEFAULT_WORKER_PIDS,
    "result_ttl": SETTINGS_DEFAULT_RESULT_TTL,
    "blocking_mode": SETTINGS_DEFAULT_BLOCKING_MODE,
}

BOOLEAN_SETTINGS = ["monitor_lock", "autocron_lock", "blocking_mode"]

# Status codes used for task-status the result-entries:
TASK_STATUS_WAITING = 1
TASK_STATUS_PROCESSING = 2
TASK_STATUS_READY = 3
TASK_STATUS_ERROR = 4
TASK_STATUS_UNAVAILABLE = 5

STATUS_MESSAGES = {
    TASK_STATUS_WAITING: "waiting",
    TASK_STATUS_PROCESSING: "processing",
    TASK_STATUS_READY: "ready",
    TASK_STATUS_ERROR: "error",
    TASK_STATUS_UNAVAILABLE: "unavailable",
}

STATUS_MESSAGE_MAX_LEN = len(max(STATUS_MESSAGES.values(), key=len))


# sqlite3 default adapters and converters deprecated as of Python 3.12:
def datetime_adapter(value):
    """
    Gets a python datetime-instance and returns an ISO 8601 formated
    string for sqlite3 storage.
    """
    return value.isoformat()


def datetime_converter(value):
    """
    Gets an ISO 8601 formated byte-string (from sqlite3) and returns a
    python datetime datatype.
    """
    return datetime.datetime.fromisoformat(value.decode())


sqlite3.register_adapter(datetime.datetime, datetime_adapter)
sqlite3.register_converter("datetime", datetime_converter)


# sqlite3: decorator for SQLiteInterface-methods accessing the database
def db_access(function):
    """
    Access decorator. Repeats the decorated function several times in
    case the database is locked because of write- or exclusive-access.
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        """
        Repeat the wrapped function call in case of an OperationalError.
        If this fails for SQLITE_MAX_RETRY_LIMIT times, the original
        error is raised.
        """
        message = ""
        delay = SQLITE_OPERATIONAL_ERROR_DELAY
        for retry_num in range(SQLITE_OPERATIONAL_ERROR_RETRIES):
            try:
                return function(*args, **kwargs)
            except sqlite3.OperationalError as err:
                message = str(err)
                time.sleep(delay)
            if not retry_num % SQLITE_DELAY_INCREMENT_STEPS:
                delay *= SQLITE_DELAY_INCREMENT_FACTOR
        raise sqlite3.OperationalError(message)

    return wrapper


class Model:
    """
    Base model for the table classes for common sql-creation and access
    methods.
    """

    # class attributes to redefine in subclasses:
    table_name = ""
    columns = {}

    def __init__(self, connection=None):
        self.connection = connection
        self.rowid = None

    def store(self):
        """
        Store a new row. data is a dictionary with all column data.
        After storage the instance-attribute `rowid` will be set.
        """
        columns = ",".join(f":{name}" for name in self.columns)
        sql = f"""INSERT INTO {self.table_name} VALUES ({columns})
                  RETURNING rowid"""
        cursor = self.connection.run(sql, self.__dict__)
        result = cursor.fetchone()
        # result is a tuple representing the RETURNING values
        # from the sql command. In this case it is a tuple with
        # a single entry holding the new created rowid:
        self.rowid = result[0]
        return self.rowid

    def update(self):
        """Make the current set of attributes persistent."""
        columns = ",".join(f"{name} = :{name}" for name in self.columns)
        sql = f"""UPDATE {self.table_name} SET {columns}
                  WHERE rowid == :rowid"""
        self.connection.run(sql, self.__dict__)

    def delete(self):
        """Delete this instance by the rowid."""
        sql = f"DELETE FROM {self.table_name} WHERE rowid == {self.rowid}"
        self.connection.run(sql)

    @classmethod
    def _get_sql_select(cls):
        """Helper function for the select-classmethods."""
        columns = list(cls.columns.keys())
        columns.append("rowid")
        columns = ",".join(columns)
        return f"SELECT {columns} FROM {cls.table_name}"

    @classmethod
    def select(cls, connection, rowid=None, sql=None, data=None):
        """
        Returns an instance depending on the arguments or None if no
        entry matches. If just `rowid` is given makes a lookup by the
        `rowid`. Otherwise `sql` and `data` must be given to make
        specific selection.
        """
        instance = None
        if rowid is not None:
            # simple select by rowid
            sql = cls._get_sql_select()
            sql = f"{sql} WHERE rowid == :rowid"
            data = {"rowid": rowid}
        cursor = connection.run(sql, data)
        cursor.row_factory = getattr(cls, "row_factory", None)
        if data := cursor.fetchone():
            instance = cls(connection)
            instance.__dict__.update(data)
        return instance

    @classmethod
    def select_all(cls, connection):
        """
        Select all entries from a table and returns a list of
        model-instances. If there is no entry in the table an empty list
        is returned.
        """
        sql = cls._get_sql_select()
        cursor = connection.run(sql)
        cursor.row_factory = getattr(cls, "row_factory", None)
        data_set = cursor.fetchall()
        instances = []
        for data in data_set:
            instance = cls(connection)
            instance.__dict__.update(data)
            instances.append(instance)
        return instances

    @classmethod
    def create_table(cls, connection):
        """Create the database table for the model if not already existing."""
        columns = ",".join(
            f"{field} {type}" for field, type in cls.columns.items()
        )
        connection.run(
            f"CREATE TABLE IF NOT EXISTS {cls.table_name}({columns})"
        )

    @classmethod
    def count_rows(cls, connection):
        """Return the number of rows in the table."""
        cursor = connection.run(f"SELECT COUNT(*) FROM {cls.table_name}")
        return cursor.fetchone()[0]


class Task(Model):
    """Model to store functions for later execution."""

    table_name = "task"
    columns = {
        "uuid": "TEXT",
        "schedule": "datetime",
        "status": "INTEGER",
        "crontab": "TEXT",
        "function_module": "TEXT",
        "function_name": "TEXT",
        "function_arguments": "BLOB",
    }

    def __init__(
        self,
        connection=None,
        func=None,
        args=(),
        kwargs=None,
        uuid="",
        crontab="",
        schedule=None,
        status=TASK_STATUS_WAITING,
        function_name="",
        function_module="",
    ):
        super().__init__(connection=connection)
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.uuid = uuid
        self.crontab = crontab
        self.schedule = schedule
        self.status = status
        self.function_module = function_module
        self.function_name = function_name
        self.function_arguments = None

    def __str__(self):
        return (
            f"{self.schedule:%Y-%m-%d %H:%M:%S} "
            f"{self.function_module}.{self.function_name} "
            f"{self.args} {self.kwargs}"
        )

    def store(self):
        """
        Store a new task in the database. Returns the rowid of the new dataset.
        """
        if self.schedule is None:
            self.schedule = datetime.datetime.now()
        if self.func:
            self.function_module = self.func.__module__
            self.function_name = self.func.__name__
        self.function_arguments = pickle.dumps((self.args, self.kwargs))
        super().store()

    def update(self):
        """Make the current state of attributes persistent."""
        # function arguments may have changed:
        self.function_arguments = pickle.dumps((self.args, self.kwargs))
        super().update()

    @classmethod
    def _get_next_task_sql_and_data(cls):
        """Helper method for next_task and next_cron_task."""
        sql = cls._get_sql_select()
        sql = f"""{sql} WHERE schedule <= :schedule
                  AND status == {TASK_STATUS_WAITING}"""
        data = {"schedule": datetime.datetime.now()}
        return sql, data

    @classmethod
    def get_by_function_name(cls, function, connection):
        """
        Return a task instance identified by the given function or None
        if no task is found.
        """
        sql = cls._get_sql_select()
        sql = f"""{sql} WHERE function_module == :function_module
                        AND function_name == :function_name"""
        data = {
            "function_module": function.__module__,
            "function_name": function.__name__,
        }
        return cls.select(connection=connection, sql=sql, data=data)

    @classmethod
    def next_task(cls, connection):
        """Returns a task instance which is on due."""
        sql, data = cls._get_next_task_sql_and_data()
        return cls.select(connection, sql=sql, data=data)

    @classmethod
    def next_cron_task(cls, connection):
        """Returns a crontask instance which is on due."""
        sql, data = cls._get_next_task_sql_and_data()
        sql = f"{sql} AND crontab <> ''"
        return cls.select(connection, sql=sql, data=data)

    @classmethod
    def delete_crontasks(cls, connection):
        """Delete all task which are cron-tasks."""
        sql = f"DELETE FROM {cls.table_name} WHERE crontab <> ''"
        connection.run(sql)

    @classmethod
    def change_status(cls, connection, prev_status, new_status):
        """Change the status of a task from a given one to a new one."""
        sql = f"""UPDATE {cls.table_name} SET status = :new_status
                  WHERE status == :prev_status"""
        data = {"prev_status": prev_status, "new_status": new_status}
        connection.run(sql, data)

    @staticmethod
    def row_factory(cursor, row):
        """
        SQLite factory class to convert a row from a task-table to a
        dictionary.
        """
        function_arguments_column_name = "function_arguments"
        data = {}
        column_names = [entry[0] for entry in cursor.description]
        for name, value in zip(column_names, row):
            if name == function_arguments_column_name:
                args, kwargs = pickle.loads(value)
                data["args"] = args
                data["kwargs"] = kwargs
            else:
                data[name] = value
        return data


class Result(Model):
    """The model to store function result of delayed executed tasks."""

    table_name = "result"
    columns = {
        "uuid": "TEXT PRIMARY KEY",
        "status": "INTEGER",
        "function_module": "TEXT",
        "function_name": "TEXT",
        "function_arguments": "BLOB",
        "function_result": "BLOB",
        "error_message": "TEXT",
        "ttl": "datetime",
    }

    def __init__(
        self,
        connection=None,
        func=None,
        args=(),
        kwargs=None,
        function_result=None,
        error_message="",
        uuid="",
        status=TASK_STATUS_WAITING,
        ttl=None,
        function_name="",
        function_module="",
        function_arguments=None,
        rowid=None,
    ):
        super().__init__(connection=connection)
        self.rowid = rowid
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.uuid = uuid
        self.status = status
        self.function_name = function_name
        self.function_module = function_module
        self.function_arguments = function_arguments
        self.function_result = function_result
        self.error_message = error_message
        self.ttl = ttl if ttl else datetime.datetime.now()

    def __str__(self):
        status = STATUS_MESSAGES[self.status]
        indent = " " * (STATUS_MESSAGE_MAX_LEN + 2)
        if self.error_message:
            result = self.error_message
        else:
            result = self.function_result
        message = (
            f"{status:<{STATUS_MESSAGE_MAX_LEN}}: "
            f"{self.function_module}.{self.function_name}"
        )
        if self.function_arguments:
            args, kwargs = self.function_arguments
            message += f"\n{indent}{args} {kwargs}"
        if result:
            message += f"\n{indent}{result}"
        return message

    @property
    def has_error(self):
        """
        Returns True if the error message is not empty. The return value
        is invalid as long as ``is_ready()`` does not return True.
        """
        return bool(self.error_message)

    @property
    def result(self):
        """
        Shortcut for the attribute ``function_result``. The return value is invalid as
        long as ``is_ready()`` does not return True.
        """
        return self.function_result

    def is_ready(self):
        """
        Returns ``True`` if the task has been processed, otherwise ``False``. If
        the task has been processed the result may be available or an
        error-message may be set.

        Note for async frameworks: this is a *blocking call*.
        """
        processed_states = (TASK_STATUS_READY, TASK_STATUS_ERROR)
        if self.status not in processed_states:
            # connect to database for a refresh of the data
            interface = SQLiteInterface()
            result = interface.get_result_by_uuid(uuid=self.uuid)
            if result is not None:
                self.__dict__.update(result.__dict__)
            return self.status in processed_states
        return True

    def store(self):
        """Stores the result as a new entry in the result-table."""
        if self.func:
            self.function_module = self.func.__module__
            self.function_name = self.func.__name__
        else:
            self.function_module = ""
            self.function_name = ""
        self.function_arguments = pickle.dumps((self.args, self.kwargs))
        self.function_result = pickle.dumps(self.function_result)
        super().store()

    @classmethod
    def from_registration(
        cls,
        func,
        args,
        kwargs,
        uuid="",
        status=TASK_STATUS_WAITING,
        function_result=None,
        error_message="",
    ):
        """
        Return a new instance with the given arguments as attributes.
        """
        return cls(
            func=func,
            args=args,
            kwargs=kwargs,
            uuid=uuid,
            function_result=function_result,
            status=status,
            error_message=error_message,
        )

    @classmethod
    def from_uuid(cls, connection, uuid):
        """
        Returns a Result instance from the database with the given uuid.
        If there is no entry return None.
        """
        sql = cls._get_sql_select()
        sql = f"{sql} WHERE uuid == :uuid"
        data = {"uuid": uuid}
        return cls.select(connection, sql=sql, data=data)

    @classmethod
    def delete_outdated(cls, connection, schedule):
        """
        Delete all result entries where the ttl is <= schedule.
        (ttl is a datetime object and works as a timestamp for storage.)
        """
        sql = f"""DELETE FROM {cls.table_name}
                  WHERE status <> {TASK_STATUS_WAITING}
                  AND ttl <= :ttl"""
        connection.run(sql, {"ttl": schedule})

    @staticmethod
    def row_factory(cursor, row):
        """
        SQLite factory class to convert a row from the result-table to a
        dictionary.
        """
        data = {}
        column_names = [entry[0] for entry in cursor.description]
        for name, value in zip(column_names, row):
            if name in ("function_arguments", "function_result"):
                data[name] = pickle.loads(value)
            else:
                data[name] = value
        return data


class Settings(Model):
    """
    Model with a single entry in the database storing the settings.
    """

    table_name = "settings"
    columns = {
        "max_workers": "INTEGER",
        "running_workers": "INTEGER",
        "monitor_lock": "INTEGER",
        "autocron_lock": "INTEGER",
        "blocking_mode": "INTEGER",
        "monitor_idle_time": "INTEGER",
        "worker_idle_time": "INTEGER",
        "worker_pids": "TEXT",
        "result_ttl": "INTEGER",
    }

    def __init__(self, connection=None, data=None):
        """
        Initializes settings with the values from data. data must be a
        dictionary with key value pairs according the `columns` class
        attribute. If data is not given the
        SETTINGS_DEFAULT_DATA-dictionary is used to populate the
        settings data.
        """
        super().__init__(connection)
        data = data if data else SETTINGS_DEFAULT_DATA
        self.__dict__.update(data)

    def __repr__(self):
        """Self representation used by the admin-tool."""
        width = len(max(self.columns, key=len))
        attributes = []
        for key in self.columns:
            value = self.__dict__[key]
            attributes.append(f"{key:<{width}}: {value}")
        return "\n".join(attributes)

    @classmethod
    def read(cls, connection):
        """
        Returns a settings instance with data read from the database. If
        there is no settings entry in the table returns None.
        """
        entries = cls.select_all(connection)
        if not entries:
            return None
        return entries[0]

    @staticmethod
    def row_factory(cursor, row):
        """
        SQLite factory function to convert a row from a settings-table
        to a dictionary.
        """
        column_names = [entry[0] for entry in cursor.description]
        data = {
            name: bool(value) if name in BOOLEAN_SETTINGS else value
            for name, value in zip(column_names, row)
        }
        return data


class TaskRegistrator:
    """
    Handles the task registration in a separate thread so that
    registration is a non-blocking operation.
    """

    def __init__(self, interface):
        self.interface = interface
        self.task_queue = queue.Queue()
        self.exit_event = threading.Event()
        self.registration_thread = None

    def register(
        self, func, args=(), kwargs=None, crontab="", uuid="", schedule=None
    ):
        """
        Register a task for later processing. Arguments are the same as
        for `SQLiteInterface.register_task()` which is called from a
        separate thread.
        """
        if kwargs is None:
            kwargs = {}
        data = {
            "func": func,
            "schedule": schedule,
            "crontab": crontab,
            "uuid": uuid,
            "args": args,
            "kwargs": kwargs,
        }
        if self.registration_thread:
            self.task_queue.put(data)
        else:
            # on not running a thread this is a blocking operation!
            self.interface.register_task(**data)

    def _process_queue(self):
        """
        Register task in a separate thread taking the tasks from a
        task_queue.
        """
        while True:
            try:
                data = self.task_queue.get(
                    timeout=REGISTER_BACKGROUND_TASK_TIMEOUT
                )
            except queue.Empty:
                # check for exit_event on empty queue so the queue items
                # can get handled before terminating the thread
                if self.exit_event.is_set():
                    break
            else:
                self.interface.register_task(**data)

    def start(self):
        """
        Start processing the queue in a seperate thread.
        """
        # don't start multiple threads
        if self.registration_thread is None:
            self.registration_thread = threading.Thread(
                target=self._process_queue
            )
            self.registration_thread.start()

    def stop(self):
        """
        Terminates the running registration thread.
        """
        if self.registration_thread:
            self.exit_event.set()
            self.registration_thread = None


class SQLiteConnection:
    """
    SQLite connection. `run()` can get called as often as required. The
    database keeps connected. Leaving the context will commit and close
    the database connection. In case of an exception during the context
    instead of a commit the connection will do a rollback.
    """

    def __init__(self, db_name, row_factory=None, exclusive=False):
        self.row_factory = row_factory
        self.db_name = db_name
        self.connection = None
        self.exclusive = exclusive

    def __enter__(self):
        self.connection = sqlite3.connect(
            database=self.db_name, detect_types=sqlite3.PARSE_DECLTYPES
        )
        if self.row_factory:
            self.connection.row_factory = self.row_factory
        if self.exclusive:
            self.connection.execute(SQLITE_EXCLUSIVE_ACCESS)
        return self

    def __exit__(self, *args):
        if any(args):
            # there was an exception:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()

    def run(self, command, parameters=(), many=None):
        """
        Runs an sql command with the given parameters. If the command
        supports qmark style, the parameters must be a tuple with the
        parameters in the proper order. If many is True, the parameters
        must be a sequence of tuples.
        If the command supports named style, parameters should be a
        dictionary. If many is True, parameters should be a sequence of
        dicts.
        Returns the result of the given command.
        """
        if parameters and many is None:
            # try to find out whether it is many or not:
            if isinstance(parameters, (tuple, list)):
                # could be qmark parameters or a sequence of tuples or dicts
                # take the first element of the sequence, if this is another
                # sequence or dict then these are parameters for an
                # executemany() command, else just execute()
                many = isinstance(parameters[0], (tuple, list, dict))
        if many:
            return self.connection.executemany(command, parameters)
        return self.connection.execute(command, parameters)


Connection = SQLiteConnection


class SQLiteInterface:
    """
    SQLite interface for application specific operations.
    This class is a Singleton.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # run __init__ just once
        if self.__dict__:
            return
        self._db_name = None
        self._result_ttl = None
        # accept_registrations will get set to False
        # by the worker processes to not register callables by the workers
        self.accept_registrations = True
        # these attributes are set later by reading the settings:
        self.autocron_lock = None
        self.monitor_lock = None
        self.monitor_idle_time = None
        self.max_workers = None
        self.worker_idle_time = None
        self.blocking_mode = None
        self.orig_signal_handlers = {}
        self.set_signal_handlers()
        # the registrator for non blocking registration:
        self.registrator = TaskRegistrator(self)
        self.init_database(f"{TEMPORARY_PREFIX}{str(uuid.uuid4())}.db")

    @property
    def db_name(self):
        """Return hidden _db_name (because of the setter)"""
        return self._db_name

    @db_name.setter
    def db_name(self, db_name):
        """
        Expand the given database filename to the storage-location path
        if the filename is not an absolute path. In the latter case the
        absolute path will get used (and must exist). If db_name is None
        set None directly.
        """
        if db_name is None:
            self._db_name = None
        else:
            path = pathlib.Path(db_name)
            if not path.is_absolute():
                try:
                    path = pathlib.Path.home() / DEFAULT_STORAGE / path.name
                except RuntimeError:
                    # no home directory found
                    path = pathlib.Path.cwd() / db_name
                else:
                    # create DEFAULT_STORAGE in home directory if not existing:
                    path.parent.mkdir(exist_ok=True)
            self._db_name = path

    @property
    def result_ttl(self):
        """
        Returns the new ttl as a datetime instance with an offset of now.
        """
        return datetime.datetime.now() + self._result_ttl

    @result_ttl.setter
    def result_ttl(self, value=SETTINGS_DEFAULT_RESULT_TTL):
        """
        Set the ttl as timedelta in seconds. This is the lifetime from
        an offset to the future.
        """
        self._result_ttl = datetime.timedelta(seconds=value)

    @property
    def has_temporary_database(self):
        if self.db_name is not None:
            if self.db_name.name.startswith(TEMPORARY_PREFIX):
                return True
        return False

    @db_access
    def init_database(self, db_name):
        """
        Set the database name and set up initial data.
        """
        if self.has_temporary_database:
            tasks = self.get_tasks()
            self._delete_database()
        else:
            tasks = []
        self.db_name = db_name
        with Connection(self.db_name, exclusive=True) as conn:
            Task.create_table(conn)
            Result.create_table(conn)
            Settings.create_table(conn)

            # try to read the settings. If this fails create the first
            # (and only) settings dataset with the default values:
            settings = Settings.read(conn)
            if settings is None:
                settings = Settings(conn)  # this set the defaults
                settings.store()  # store defaults in the database

            # set attributes from settings that don't change during runtime:
            self.autocron_lock = settings.autocron_lock
            self.monitor_lock = settings.monitor_lock
            self.monitor_idle_time = settings.monitor_idle_time
            self.max_workers = settings.max_workers
            self.worker_idle_time = settings.worker_idle_time
            self.result_ttl = settings.result_ttl
            self.blocking_mode = settings.blocking_mode

            # copy the tasks if any:
            for task in tasks:
                # set connection from the closed one to the new one:
                task.connection = conn
                task.store()

    @db_access
    def register_task(
        self, func, schedule=None, crontab="", uuid="", args=(), kwargs=None
    ):
        """
        Store a callable in the task-table of the database. If the
        callable is a delayed task with a potential result create also a
        corresponding entry in the result table. If the callable is a
        crontask, check whether the task is already registered and don't
        register the callable again.
        """
        if self.accept_registrations:
            if not schedule:
                schedule = datetime.datetime.now()
            if kwargs is None:
                kwargs = {}
            with Connection(self.db_name, exclusive=True) as conn:
                if crontab and Task.get_by_function_name(func, conn):
                    # don't register a crontab twice:
                    return
                task = Task(
                    connection=conn,
                    func=func,
                    schedule=schedule,
                    crontab=crontab,
                    uuid=uuid,
                    args=args,
                    kwargs=kwargs,
                )
                task.store()

                # if a uuid is given it is a delayed function that
                # may return a result:
                if uuid:
                    result = Result(
                        connection=conn,
                        func=func,
                        args=args,
                        kwargs=kwargs,
                        uuid=uuid,
                        ttl=self.result_ttl,
                    )
                    result.store()

    @db_access
    def get_next_task(self):
        """
        Returns the next task on due with crontasks first or None if
        there is not task on due. If a task is returned the status is
        set to TASK_STATUS_PROCESSING first.
        """
        with Connection(self.db_name, exclusive=True) as conn:
            task = Task.next_cron_task(conn) or Task.next_task(conn)
            if task:
                task.status = TASK_STATUS_PROCESSING
                task.update()
        return task

    @db_access
    def update_task_schedule(self, task, schedule):
        """Updates the schedule of the given task."""
        with Connection(self.db_name) as conn:
            task.connection = conn
            task.schedule = schedule
            task.status = TASK_STATUS_WAITING
            task.update()

    @db_access
    def count_tasks(self):
        """Return the number of entries in the task-table."""
        with Connection(self.db_name) as conn:
            return Task.count_rows(conn)

    @db_access
    def get_tasks(self):
        """Return a list of all tasks."""
        with Connection(self.db_name) as conn:
            return Task.select_all(conn)

    @db_access
    def delete_task(self, task):
        """Delete the task which may not have a valid connection-attribute."""
        # solution: inject a valid connection
        with Connection(self.db_name) as conn:
            task.connection = conn
            task.delete()

    @db_access
    def get_results(self):
        """Return a list of all results."""
        with Connection(self.db_name) as conn:
            return Result.select_all(conn)

    @db_access
    def get_result_by_uuid(self, uuid):
        """
        Return a Result instance from the database identified by the uuid.
        """
        with Connection(self.db_name) as conn:
            return Result.from_uuid(connection=conn, uuid=uuid)

    @db_access
    def count_results(self):
        """Return the number of entries in the task-table."""
        with Connection(self.db_name) as conn:
            return Result.count_rows(conn)

    @db_access
    def update_result(self, uuid, result=None, error_message="", ttl=None):
        """
        Updates the result with the uuid with the values of the
        arguments result and error_message.
        """
        function_result = pickle.dumps(result)
        ttl = ttl if ttl else self.result_ttl
        status = TASK_STATUS_ERROR if error_message else TASK_STATUS_READY
        with Connection(self.db_name) as conn:
            result = Result.from_uuid(conn, uuid=uuid)
            result.function_result = function_result
            result.function_arguments = pickle.dumps(result.function_arguments)
            result.error_message = error_message
            result.status = status
            result.ttl = ttl
            result.update()

    @db_access
    def delete_outdated_results(self):
        """Delete all resuts with a ttl <= now."""
        with Connection(self.db_name) as conn:
            Result.delete_outdated(conn, datetime.datetime.now())

    @db_access
    def increment_running_workers(self, pid):
        """
        Add the pid to the worker pid-list and increase the running
        worker num by 1.
        """
        with Connection(self.db_name) as conn:
            settings = Settings.read(connection=conn)
            settings.worker_pids = f"{settings.worker_pids},{pid}".lstrip(",")
            settings.running_workers += 1
            settings.update()

    @db_access
    def decrement_running_workers(self, pid):
        """
        Delete the pid from the worker_pids list and decrement the
        running_workers counter.
        """
        with Connection(self.db_name) as conn:
            settings = Settings.read(connection=conn)
            pids = settings.worker_pids.split(",")
            try:
                pids.remove(str(pid))
            except ValueError:
                # pid not in list: ignore
                pass
            else:
                settings.worker_pids = ",".join(pids)
                settings.running_workers = len(pids)
                settings.update()

    @db_access
    def is_worker_pid(self, pid):
        """Check whether the provided pid is one of the worker pids."""
        with Connection(self.db_name) as conn:
            settings = Settings.read(connection=conn)
        pids = (int(p) for p in settings.worker_pids.split(",") if p)
        return pid in pids

    @db_access
    def acquire_monitor_lock(self):
        """
        Tries to acquire the monitor-lock flag: if the flag is set to
        False, then set it to True. Otherwise keep the flag as is.
        Return True if the flag has been set to True, otherwise return
        False.
        """
        with Connection(self.db_name, exclusive=True) as conn:
            settings = Settings.read(connection=conn)
            if not settings.monitor_lock:
                settings.monitor_lock = True
                settings.update()
                return True
            return False

    @db_access
    def get_settings(self):
        """Returns the settings dataset."""
        with Connection(self.db_name) as conn:
            return Settings.read(connection=conn)

    @db_access
    def update_settings(self, settings):
        """Updates the settings dataset."""
        with Connection(self.db_name) as conn:
            settings.connection = conn
            settings.update()

    @db_access
    def tear_down_database(self):
        """
        Reset all settings here so that the workers don't have to access
        the database again on shutdown. Gets called from the engine on
        shut-down.
        """
        with Connection(self.db_name, exclusive=True) as conn:
            settings = Settings.read(conn)
            settings.monitor_lock = False
            settings.running_workers = 0
            settings.worker_pids = ""
            settings.update()
            Task.delete_crontasks(conn)
            # reset the status of unfinished tasks from the
            # last run to handle them again:
            Task.change_status(
                conn,
                prev_status=TASK_STATUS_PROCESSING,
                new_status=TASK_STATUS_WAITING,
            )

    @db_access
    def _delete_database(self):
        """
        Internal command to delete the temporary databases needed for start-up.
        """
        if self.db_name is not None:
            db_path = pathlib.Path(self.db_name)
            db_path.unlink(missing_ok=True)

    def __del__(self):
        # last resort additional to the signal handler
        # not guaranteed to get called but useful in tests
        # circumventing the singleton pattern
        if self.has_temporary_database:
            self._delete_database()

    def _shut_down(self, signalnum, stackframe=None):
        if self.has_temporary_database:
            self._delete_database()
        self.reset_signal_handlers()
        signal.raise_signal(signalnum)  # requires Python >= 3.8

    def set_signal_handlers(self):
        """
        Set self._terminate() as handler for a couple of
        termination-signals and store the orinal handlers for this
        signals.
        """
        signalnums = [
            signal.SIGINT,
            signal.SIGTERM,
        ]
        for signalnum in signalnums:
            self.orig_signal_handlers[signalnum] = signal.getsignal(signalnum)
            signal.signal(signalnum, self._shut_down)

    def reset_signal_handlers(self):
        """
        Reset the original signal handlers.
        """
        for signalnum, signalhandler in self.orig_signal_handlers.items():
            signal.signal(signalnum, signalhandler)
