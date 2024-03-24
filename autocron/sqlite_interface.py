
import datetime
import pathlib
import pickle
import sqlite3
import time


DEFAULT_STORAGE = ".autocron"

SQLITE_OPERATIONAL_ERROR_RETRIES = 100
SQLITE_OPERATIONAL_ERROR_DELAY = 0.01
SQLITE_DELAY_INCREMENT_STEPS = 20
SQLITE_DELAY_INCREMENT_FACTOR = 1.5

SQLITE_EXCLUSIVE_ACCESS = "BEGIN EXCLUSIVE"

SETTINGS_DEFAULT_WORKERS = 1
SETTINGS_DEFAULT_RUNNING_WORKERS = 0
SETTINGS_DEFAULT_MONITOR_LOCK = 0
SETTINGS_DEFAULT_AUTOCRON_LOCK = 0
SETTINGS_DEFAULT_MONITOR_IDLE_TIME = 5  # seconds
SETTINGS_DEFAULT_WORKER_IDLE_TIME = 0  # 0 seconds means auto idle time
SETTINGS_DEFAULT_WORKER_PIDS = ""
SETTINGS_DEFAULT_RESULT_TTL = 1800  # Storage time (time to live) in seconds

SETTINGS_DEFAULT_DATA = {
    "max_workers": SETTINGS_DEFAULT_WORKERS,
    "running_workers": SETTINGS_DEFAULT_RUNNING_WORKERS,
    "monitor_lock": SETTINGS_DEFAULT_MONITOR_LOCK,
    "autocron_lock": SETTINGS_DEFAULT_AUTOCRON_LOCK,
    "monitor_idle_time": SETTINGS_DEFAULT_MONITOR_IDLE_TIME,
    "worker_idle_time": SETTINGS_DEFAULT_WORKER_IDLE_TIME,
    "worker_pids": SETTINGS_DEFAULT_WORKER_PIDS,
    "result_ttl": SETTINGS_DEFAULT_RESULT_TTL
}

BOOLEAN_SETTINGS = ["monitor_lock", "autocron_lock"]

# Status codes used for task-status the result-entries:
TASK_STATUS_WAITING = 1
TASK_STATUS_PROCESSING = 2
TASK_STATUS_READY = 3
TASK_STATUS_ERROR = 4
TASK_STATUS_UNAVAILABLE = 5


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

    def __init__(self, connection=None):
        self.connection = connection
        self.rowid = None

    def get_sql_create_table(self):
        columns = ",".join(
            f"{name} {datatype}" for name, datatype in self.columns.items()
        )
        return f"CREATE TABLE IF NOT EXISTS {self.table_name}({columns})"

    def get_sql_store_all_columns(self):
        """Returns the sql to store all columns of a table in named style.
        """
        columns = ",".join(
            f":{name}" for name in self.columns.keys()
        )
        return f"""INSERT INTO {self.table_name} VALUES ({columns})
                   RETURNING rowid"""

    def get_sql_update(self, columns, id_name):
        """
        Returns the sql required to update the given columns (a list of
        strings). id_name is column name used as key and id_value is the
        corresonding value.
        """
        columns = ",".join(f"{name} = :{name}" for name in columns)
        return f"""UPDATE {self.table_name} SET {columns}
                   WHERE {id_name} == :id_value"""

    def get_sql_read_all_columns(self):
        """
        Returns the sql to read all columns from a given table including
        the sqlite specific rowid.
        """
        # should also read the rowid, so SELECT * FROM can't be used
        columns = list(self.columns.keys())
        columns.append("rowid")
        columns = ",".join(columns)
        return f"SELECT {columns} FROM {self.table_name}"

    def get_sql_delete(self):
        """Returns the sql to delete an entry based on the rowid.
        """
        return f"DELETE FROM {self.table_name} WHERE rowid == {self.rowid}"

    def store(self, data):
        """
        Store a new row. data is a dictionary with all column data.
        After storage the instance-attribute `rowid` will be set.
        """
        sql = self.get_sql_store_all_columns()
        cursor = self.connection.run(sql, data)
        result = cursor.fetchone()
        # result is a tuple representing the RETURNING values
        # from the sql command. In this case it is tuple with
        # a single entry holding the new created rowid:
        self.rowid = result[0]

    def update(self, id_name=None, id_value=None, **kwargs):
        """
        Updates the fields given as keyword arguments with the
        corresponding values. `id_name` is the column name to use for selection
        """
        if id_name is None:
            id_name = "rowid"
        if id_value is None:
            id_value = self.rowid
        sql = self.get_sql_update(kwargs.keys(), id_name)
        kwargs["id_value"] = id_value
        self.connection.run(sql, parameters=kwargs)

    def delete(self):
        """Delete the item from the database-table.
        """
        self.connection.run(self.get_sql_delete())

    @classmethod
    def create_table(cls, connection):
        sql = cls.get_sql_create_table(cls)
        connection.run(sql)

    @classmethod
    def count_rows(cls, connection):
        """Return the number of rows in the table.
        """
        sql = f"SELECT COUNT(*) FROM {cls.table_name}"
        cursor = connection.run(sql)
        rows = cursor.fetchone()[0]
        return rows

    @classmethod
    def read_all(cls, connection):
        """
        Returns a list of entries from the table-class. The
        connection-attribute is set to None because it would be invalide
        anyway.
        """
        sql = cls.get_sql_read_all_columns(cls)
        cursor = connection.run(sql)
        cursor.row_factory = cls.row_factory
        entries = []
        for data in cursor.fetchall():
            entry = cls()
            entry.__dict__.update(data)
            entries.append(entry)
        return entries

    @classmethod
    def change_status(cls, connection, prev_status, new_status):
        """Change status of all entries from a given status to a new one.
        """
        sql = f"""UPDATE {cls.table_name} SET status = :new_status
                  WHERE status == :prev_status"""
        connection.run(sql, locals())


class Task(Model):

    table_name = "task"
    columns = {
        "uuid": "TEXT",
        "schedule": "datetime",
        "status": "INTEGER",
        "crontab": "TEXT",
        "function_module": "TEXT",
        "function_name": "TEXT",
        "function_arguments": "BLOB"
    }

    def get_sql_tasks_on_due(self):
        sql = self.get_sql_read_all_columns()
        return f"""{sql} WHERE schedule <= :schedule
                   AND status == {TASK_STATUS_WAITING}"""

    def get_sql_crontasks_on_due(self):
        sql = self.get_sql_tasks_on_due()
        return f"{sql} AND crontab <> ''"

    def get_sql_select_by_status(self):
        sql = self.get_sql_read_all_columns()
        return f"{sql} WHERE status == :status"

    def _get_next_task_on_due(self, sql, schedule):
        parameters = {"schedule": schedule}
        cursor = self.connection.run(sql, parameters=parameters)
        cursor.row_factory = self.row_factory
        data = cursor.fetchone()
        if not data:
            return None
        self.__dict__.update(data)
        return self

    def read_next_task(self, schedule):
        """
        Reads the data of the next task into the instance. Returns the
        instance or None, if no task on has been found.
        """
        sql = self.get_sql_tasks_on_due()
        return self._get_next_task_on_due(sql, schedule)

    def read_next_crontask(self, schedule):
        """
        Reads the data of the next crontask into the instance. Returns
        the instance or None, if no task on has been found.
        """
        sql = self.get_sql_crontasks_on_due()
        return self._get_next_task_on_due(sql, schedule)

    def store(self, func, schedule=None, crontab="", uuid="",
              args=(), kwargs=None):
        """
        Store a new task in the database and return the rowid of the
        created entry.
        """
        if schedule is None:
            schedule = datetime.datetime.now()
        data = {
            "uuid": uuid,
            "schedule": schedule,
            "status": TASK_STATUS_WAITING,
            "crontab": crontab,
            "function_module": func.__module__,
            "function_name": func.__name__,
            "function_arguments": pickle.dumps((args, kwargs))
        }
        super().store(data)

    @classmethod
    def delete_crontasks(cls, connection):
        """Delete all task which are cron-tasks.
        """
        sql = f"DELETE FROM {cls.table_name} WHERE crontab <> ''"
        connection.run(sql)

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

    table_name = "result"
    columns = {
        "uuid": "TEXT PRIMARY KEY",
        "status": "INTEGER",
        "function_module": "TEXT",
        "function_name": "TEXT",
        "function_arguments": "BLOB",
        "function_result": "BLOB",
        "error_message": "TEXT",
        "ttl": "datetime"
    }

    def store(self, func, uuid, args=(), kwargs=None):
        """
        Stores a new entry in the result table waiting to get updated
        later after executing the function. ttl is set to default and
        gets updated when the result is updated.
        """
        data = {
            "uuid": uuid,
            "status": TASK_STATUS_WAITING,
            "function_module": func.__module__,
            "function_name": func.__name__,
            "function_arguments": pickle.dumps((args, kwargs)),
            "function_result": pickle.dumps(None),
            "error_message": "",
            "ttl": SETTINGS_DEFAULT_RESULT_TTL
        }
        super().store(data)


class Settings(Model):

    table_name = "settings"
    columns = {
        "max_workers": "INTEGER",
        "running_workers": "INTEGER",
        "monitor_lock": "INTEGER",
        "autocron_lock": "INTEGER",
        "monitor_idle_time": "INTEGER",
        "worker_idle_time": "INTEGER",
        "worker_pids": "TEXT",
        "result_ttl": "INTEGER"
    }

    def read(self):
        """Read the settings from the single entry in this table.
        """
        sql = self.get_sql_read_all_columns()
        cursor = self.connection.run(sql)
        cursor.row_factory = self.row_factory
        data = cursor.fetchone()
        self.__dict__.update(data)
        return self

    @staticmethod
    def row_factory(cursor, row):
        """
        SQLite factory function to convert a row from a settings-table
        to a dictionary.
        """
        column_names = [entry[0] for entry in cursor.description]
        data = {name: bool(value) if name in BOOLEAN_SETTINGS else value
                for name, value in zip(column_names, row)}
        return data


class SQLiteConnection:
    """
    SQLite connection. `run()` can get called as often as required. The
    database keeps connected. Leaving the context will close the
    database connection.
    """

    def __init__(self, db_name, row_factory=None, exclusive=False):
        self.row_factory = row_factory
        self.db_name = db_name
        self.connection = None
        self.exclusive = exclusive

    def __enter__(self):
        self.connection = sqlite3.connect(
            database=self.db_name,
            detect_types=sqlite3.PARSE_DECLTYPES
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
        # run __init__ just on the first instance
        if self.__dict__:
            return
        self._result_ttl = None
        self._accept_registrations = True
        self._db_name = None
        self.autocron_lock_is_set = None
        self.worker_idle_time = None
        self.monitor_idle_time = None
        # if set this process controls the workers
        self.is_worker_master = False
        self.max_workers = SETTINGS_DEFAULT_WORKERS

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

    @db_access
    def init_database(self, db_name):
        """
        Set the database name and set up initial data.
        """
        if not self.db_name:
            self.db_name = db_name
            with Connection(self.db_name, exclusive=True) as conn:
                Task.create_table(conn)
                Result.create_table(conn)
                Settings.create_table(conn)

                # set default settings if no settings stored:
                settings = Settings(conn)
                if not Settings.count_rows(conn):
                    settings.store(SETTINGS_DEFAULT_DATA)

                # read settings that don't change during runtime:
                settings.read()
                self.autocron_lock_is_set = settings.autocron_lock
                self.max_workers = settings.max_workers
                self.worker_idle_time = settings.worker_idle_time
                self.monitor_idle_time = settings.monitor_idle_time
                self.result_ttl = settings.result_ttl

                # try to aquire the monitor_lock flag in case
                # autocron is active:
                if not self.autocron_lock_is_set:
                    if settings.monitor_lock is False:
                        settings.update(monitor_lock=True)
                        # this process handles the workers
                        self.is_worker_master = True

                # tasks from the last run in processing state and therefor
                # not finished are reset to waiting mode to get executed
                # again.
                Task.change_status(
                    conn,
                    prev_status=TASK_STATUS_PROCESSING,
                    new_status=TASK_STATUS_WAITING
                )

    @db_access
    def register_task(self, func, schedule=None, crontab="", uuid="",
                      args=(), kwargs=None, unique=False):
        """
        Store a callable in the task-table of the database. If the
        callable is a delayed task with a potential result create also a
        corresponding entry in the result table.
        """
        if not schedule:
            schedule = datetime.datetime.now()
        if kwargs is None:
            kwargs = {}
        with Connection(self.db_name, exclusive=True) as conn:
            task = Task(conn)
            task.store(func, schedule, crontab, uuid, args, kwargs)

            # if a uuid is given it is a delayed function that
            # may return a result:
            if uuid:
                result = Result(conn)
                result.store(func, uuid, args, kwargs)

    @db_access
    def get_next_task(self):
        """
        Returns the next task on due with crontasks first or None if
        there is not task on due. If a task is returned the status is
        set to TASK_STATUS_PROCESSING first.
        """
        schedule = datetime.datetime.now()
        with Connection(self.db_name, exclusive=True) as conn:
            task = Task(conn)
            for task_getter in (task.read_next_crontask, task.read_next_task):
                found = task_getter(schedule)
                if found:
                    break
            else:
                return None
            task.update(status=TASK_STATUS_PROCESSING)
            return task

    @db_access
    def count_tasks(self):
        """Return the number of entries in the task-table.
        """
        with Connection(self.db_name) as conn:
            return Task.count_rows(conn)

    @db_access
    def get_tasks(self):
        """Return a list of all tasks.
        """
        with Connection(self.db_name) as conn:
            return Task.read_all(conn)

    @db_access
    def shut_down_process(self):
        """
        Reset all settings here so that the workers don't have to access
        the database again on shutdown.
        """
        # gets called from the engine in case the interface
        # is the worker_master
        with Connection(self.db_name) as conn:
            settings = Settings(conn)
            settings.read()
            settings.update(
                monitor_lock=False,
                running_workers=0,
                worker_pids=""
            )
            Task.delete_crontasks(conn)
