"""
sql_interface.py

SQLite interface for storing tasks.

The autocron database consists of three tables:
task: stores all tasks that should get executed later
result: stores all results from task returning a result
settings: configuration settings for a project
"""

import datetime
import pathlib
import pickle
import sqlite3
import types


DEFAULT_STORAGE = ".autocron"


# -- table: task - structure and commands ------------------------------------
DB_TABLE_NAME_TASK = "task"
CMD_CREATE_TASK_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME_TASK}
(
    uuid TEXT,
    schedule datetime,
    status INTEGER,
    crontab TEXT,
    function_module TEXT,
    function_name TEXT,
    function_arguments BLOB
)
"""
CMD_STORE_TASK = f"""
INSERT INTO {DB_TABLE_NAME_TASK} VALUES
(
    :uuid,
    :schedule,
    :status,
    :crontab,
    :function_module,
    :function_name,
    :function_arguments
)
"""
TASK_COLUMN_SEQUENCE =\
    "rowid,uuid,schedule,status,crontab,"\
    "function_module,function_name,function_arguments"
CMD_GET_TASKS_BY_NAME =f"""
    SELECT {TASK_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK}
    WHERE function_module == ? AND function_name == ?"""
CMD_GET_TASKS_ON_DUE = f"""
    SELECT {TASK_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK}
    WHERE schedule <= ?"""
CMD_GET_TASKS_ON_DUE_WITH_STATUS = CMD_GET_TASKS_ON_DUE + " AND status == ?"
CMD_GET_TASKS = f"""
    SELECT {TASK_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK}"""
CMD_UPDATE_TASK_STATUS = f"""
    UPDATE {DB_TABLE_NAME_TASK} SET status = ? WHERE rowid == ?"""
CMD_GET_CRONTASKS = f"""
    SELECT  {TASK_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK}
    WHERE crontab <> ''"""
CMD_UPDATE_CRONTASK_SCHEDULE = f"""
    UPDATE {DB_TABLE_NAME_TASK} SET schedule = ?, status = ? WHERE rowid == ?"""
CMD_DELETE_TASK = f"DELETE FROM {DB_TABLE_NAME_TASK} WHERE rowid == ?"
CMD_DELETE_CRON_TASKS = f"DELETE FROM {DB_TABLE_NAME_TASK} WHERE crontab <> ''"
CMD_COUNT_TABLE_ROWS = "SELECT COUNT(*) FROM {table_name}"

# Status codes used for task-status the result-entries:
TASK_STATUS_WAITING = 1
TASK_STATUS_PROCESSING = 2
TASK_STATUS_READY = 3
TASK_STATUS_ERROR = 4
TASK_STATUS_UNAVAILABLE = 5


# -- table: result - structure and commands ----------------------------------
DB_TABLE_NAME_RESULT = "result"
CMD_CREATE_RESULT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME_RESULT}
(
    uuid TEXT PRIMARY KEY,
    status INTEGER,
    function_module TEXT,
    function_name TEXT,
    function_arguments BLOB,
    function_result BLOB,
    error_message TEXT,
    ttl datetime
)
"""

# Storage time (time to live) for results in seconds
RESULT_TTL = 1800

CMD_STORE_RESULT = f"""
INSERT INTO {DB_TABLE_NAME_RESULT} VALUES
(
    :uuid,
    :status,
    :function_module,
    :function_name,
    :function_arguments,
    :function_result,
    :error_message,
    :ttl
)
"""
RESULT_COLUMN_SEQUENCE =\
    "rowid,uuid,status,function_module,function_name,"\
    "function_arguments,function_result,error_message, ttl"
CMD_GET_RESULTS = f"""
    SELECT {RESULT_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_RESULT}"""
CMD_GET_RESULT_BY_UUID = f"""
    SELECT {RESULT_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_RESULT}
    WHERE uuid == ?"""
CMD_UPDATE_RESULT = f"""
    UPDATE {DB_TABLE_NAME_RESULT} SET
        status = ?,
        function_result = ?,
        error_message = ?,
        ttl = ?
    WHERE uuid == ?"""
# CMD_DELETE_RESULT = f"""\
#     DELETE FROM {DB_TABLE_NAME_RESULT} WHERE uuid == ?"""
CMD_DELETE_OUTDATED_RESULTS = f"""
    DELETE FROM {DB_TABLE_NAME_RESULT}
    WHERE status == {TASK_STATUS_READY} AND ttl <= ?"""


# -- table: settings - structure and commands --------------------------------
DB_TABLE_NAME_SETTINGS = "settings"
CMD_CREATE_SETTINGS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME_SETTINGS}
(
    max_workers INTEGER,
    running_workers INTEGER,
    monitor_lock INTEGER,
    autocron_lock INTEGER,
    monitor_idle_time REAL,
    worker_idle_time REAL,
    worker_pids TEXT,
    result_ttl INTEGER
)
"""

DEFAULT_MAX_WORKERS = 1
DEFAULT_RUNNING_WORKERS = 0
DEFAULT_MONITOR_LOCK = 0
DEFAULT_AUTOCRON_LOCK = 0
DEFAULT_MONITOR_IDLE_TIME = 5.0  # seconds
DEFAULT_WORKER_IDLE_TIME = 2.0  # seconds
DEFAULT_WORKER_PIDS = ""

CMD_SETTINGS_STORE_VALUES = f"""
INSERT INTO {DB_TABLE_NAME_SETTINGS} VALUES
(
    :max_workers,
    :running_workers,
    :monitor_lock,
    :autocron_lock,
    :monitor_idle_time,
    :worker_idle_time,
    :worker_pids,
    :result_ttl
)
"""
SETTINGS_COLUMN_SEQUENCE =\
    "rowid,max_workers,running_workers,"\
    "monitor_lock,autocron_lock,"\
    "monitor_idle_time,worker_idle_time,"\
    "worker_pids,result_ttl"
BOOLEAN_SETTINGS = ["monitor_lock", "autocron_lock"]
CMD_SETTINGS_GET_SETTINGS = f"""
    SELECT {SETTINGS_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_SETTINGS}"""
CMD_SETTINGS_UPDATE = f"""
    UPDATE {DB_TABLE_NAME_SETTINGS} SET
        max_workers = ?,
        running_workers = ?,
        monitor_lock = ?,
        autocron_lock = ?,
        monitor_idle_time = ?,
        worker_idle_time = ?,
        worker_pids = ?,
        result_ttl = ?
    WHERE rowid == ?"""


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


# pylint does not like instances with dynamic attributes:
# pylint: disable=attribute-defined-outside-init
class HybridNamespace(types.SimpleNamespace):
    """
    A namespace-object with additional dictionary-like attribute access.
    """
    def __init__(self, data=None):
        """
        Set initial values.
        If data is given, it must be a dictionary.
        """
        if data is not None:
            self.__dict__.update(data)

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, value):
        self.__dict__[name] = value

    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        return "\n".join(f"{k}:{v}" for k, v in self.__dict__.items())

    def __str__(self):
        # same as __repr__ but without the rowid
        lines = repr(self).split("\n")
        lines = [line for line in lines if not line.startswith("rowid")]
        return "\n".join(lines)


class TaskResult(HybridNamespace):
    """
    Helper class to make task-results more handy.
    """

    def _refresh(self):
        """
        If the status is TASK_STATUS_WAITING try to update the
        task_result dictionary with data retrieved from the database.
        This will return a new task_result instance. If the new instance
        is still in waiting status, do nothing. Otherwise update
        self.__dict__ with the retrieved data.
        The `interface` argument is for testing.
        """
        if self.status == TASK_STATUS_WAITING:
            try:
                result = self.interface.get_result_by_uuid(self.uuid)
            except AttributeError:
                pass
            else:
                if result is not None:
                    self.__dict__.update(result.__dict__)

    @property
    def result(self):
        """
        Shortcut to access the result. If the result is not available
        because the task still waits to get executed an AttributeError
        with the message "result not available" is raised.
        """
        if self.status == TASK_STATUS_READY:
            return self.function_result
        raise AttributeError("result not available.")

    @property
    def is_waiting(self):
        """indicates task still waiting to get processed."""
        self._refresh()
        return self.status == TASK_STATUS_WAITING

    @property
    def is_ready(self):
        """indicates task has been processed."""
        self._refresh()
        return self.status == TASK_STATUS_READY

    @property
    def has_error(self):
        """indicates error_message is set."""
        self._refresh()
        return self.status == TASK_STATUS_ERROR

    @classmethod
    def from_data_tuple(cls, row_data):
        """
        Returns a new TaskResult-Instance initialized with a tuple
        representing the data from result-table row.
        """
        column_names = RESULT_COLUMN_SEQUENCE.split(",")
        data = dict(zip(column_names, row_data))
        instance = cls(data)
        instance.function_result = pickle.loads(instance.function_result)
        instance.function_arguments = pickle.loads(instance.function_arguments)
        return instance

    @classmethod
    def from_function_call(cls, func, *args, **kwargs):
        """
        Renturns a new TaskResult-Instance with the result from the
        given function executed with the given arguments. This exists
        for type consistency to return a TaskResult from delay-decorated
        functions even if autotask is inactive.
        """
        data = {
            "function_result": func(*args, **kwargs),
            "function_arguments": (args, kwargs),
            "status": TASK_STATUS_READY
        }
        return cls(data)

    @classmethod
    def from_registration(cls, uuid, interface):
        """
        Returns a TaskResult-Instance with the state
        TASK_STATUS_WAITING, the result None and a set uuid. This
        instance can be used to call the ``update()`` method for a
        delayed result.
        """
        data = {
            "function_result": None,
            "status": TASK_STATUS_WAITING,
            "uuid": uuid,
            "interface": interface
        }
        return cls(data)

    @classmethod
    def from_none(cls):
        """
        Returns a new empty TaskResult-Instance if there is no waiting
        function to call and no result to expect. The available
        attributes are 'status' with the value TASK_STATUS_UNAVAILABLE and
        the 'result' has the value None. All other attributes are
        undefined and accessing them will raise an AttributeError.
        """
        data = {
            "status": TASK_STATUS_UNAVAILABLE,
            "function_result": None
        }
        return cls(data)


# pylint: disable=too-many-public-methods
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
        self._preregistered_tasks = []
        self._result_ttl = datetime.timedelta(seconds=RESULT_TTL)
        self._accept_registrations = True
        self._db_name = None
        self.autocron_lock_is_set = None

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
            self._set_storage_location(db_name)

    @property
    def accept_registrations(self):
        """
        Return a boolean whether callables are allowed to get registered
        in the database.
        """
        return self._accept_registrations and not self.autocron_lock_is_set

    @accept_registrations.setter
    def accept_registrations(self, value):
        """
        Setter for the hidden _accept_registrations flag (because of the
        getter)
        """
        self._accept_registrations = value

    @property
    def is_initialized(self):
        """Flag whether the database is available."""
        return self._db_name is not None

    def _init_database(self):
        """
        Creates a new database in case that an older one is not existing
        and in case of missing settings set the settings-default values.
        """
        self._create_tables()
        self._initialize_settings_table()
        settings = self.get_settings()
        self.autocron_lock_is_set = settings.autocron_lock
        self._result_ttl = datetime.timedelta(seconds=settings.result_ttl)

    def _create_tables(self):
        """
        Create all used tables in case of a new db and missing tables.
        """
        self._execute(CMD_CREATE_TASK_TABLE)
        self._execute(CMD_CREATE_RESULT_TABLE)
        self._execute(CMD_CREATE_SETTINGS_TABLE)

    def _initialize_settings_table(self):
        """
        Check for an existing settings row in the settings-table.
        If there is no row create an entry with the default values.
        """
        rows = self._count_table_rows(DB_TABLE_NAME_SETTINGS)
        if not rows:
            data = {
                "max_workers": DEFAULT_MAX_WORKERS,
                "running_workers": DEFAULT_RUNNING_WORKERS,
                "monitor_lock": DEFAULT_MONITOR_LOCK,
                "autocron_lock": DEFAULT_AUTOCRON_LOCK,
                "monitor_idle_time": DEFAULT_MONITOR_IDLE_TIME,
                "worker_idle_time": DEFAULT_WORKER_IDLE_TIME,
                "worker_pids": DEFAULT_WORKER_PIDS,
                "result_ttl": RESULT_TTL
            }
            self._execute(CMD_SETTINGS_STORE_VALUES, data)

    def _count_table_rows(self, table_name):
        """
        Helper function to count the number of entries in the given
        table. Returns a numeric value. In case of an unknown table_name
        a sqlite3.OperationalError will get raised.
        """
        cmd = CMD_COUNT_TABLE_ROWS.format(table_name=table_name)
        cursor = self._execute(cmd)
        number_of_rows = cursor.fetchone()[0]
        return number_of_rows

    def _execute(self, cmd, parameters=(), many=False):
        """
        Run a command with parameters. Parameters can be a sequence of
        values to get used in an ordered way or a dictionary with
        key-value pairs, where the key are the value-names used in the
        db (i.e. the column names).
        If 'many' is true then con.executemany() gets called and
        parameters is interpreted differently as as sequence of ordered
        tuples or dictionaries as placehoöders for the provided cmd.
        """
        if self._db_name is None:
            raise IOError("No autocron database defined.")
        con = sqlite3.connect(
            self._db_name,
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        with con:
            if many:
                return con.executemany(cmd, parameters)
            return con.execute(cmd, parameters)

    def _set_storage_location(self, db_name):
        """
        Set the database file location. If the path is absolute use the
        path as is. The directory part of the path must exist. If the
        path is relative the database file gets stored in the
        '~.autocron/' directory. If no home directory is found on the
        running platform the current working directory is used as a
        fallback. However in such cases it would be safer to provide an
        absolute path to the database location.
        """
        path = pathlib.Path(db_name)
        if not path.is_absolute():
            try:
                path = pathlib.Path.home() / DEFAULT_STORAGE / path.name
            except RuntimeError:
                # no home directory found
                path = pathlib.Path.cwd() / db_name
        self._db_name = path

    def _preregister_task(self, data):
        """
        Take the data, which is a dictionary,and convert it to another
        dict without the key 'self'. Pickle the dict and append it to
        _preregistered tasks.
        """
        data = {k: v for k, v in data.items() if k != "self"}
        self._preregistered_tasks.append(data)

    def _register_preregistered_tasks(self):
        """
        Run the stored registrations on the now up and
        running database.
        """
        for data in self._preregistered_tasks:
            self.register_callable(**data)

    def init_database(self, db_name):
        """
        Public callable for delayed initialization. Set the database
        (which is a string or Path) and runs the correponding
        initialization.
        """
        if not self.is_initialized:
            self._set_storage_location(db_name)
            self._init_database()
            # ignore the result, but set new state:
            self.get_tasks_on_due(
                status=TASK_STATUS_PROCESSING,
                new_status=TASK_STATUS_WAITING
            )
            self._register_preregistered_tasks()


    # -- task-methods ---

    @staticmethod
    def _fetch_all_callable_entries(cursor):
        """
        Internal function to iterate over a selection of entries and unpack
        the columns to a dictionary with the following key-value pairs:

            {
                "rowid": integer,
                "uuid": string,
                "schedule": datetime,
                "status": integer,
                "crontab": string,
                "function_module": string,
                "function_name": string,
                "args": tuple(of original datatypes),
                "kwargs": dict(of original datatypes),
            }

        Returns a list of HybridNamespace instances or an empty list if
        a selection does not match any row.
        """
        def process(row):
            """
            Gets a `row` and returns a dictionary with Python datatypes.
            `row` is an ordered tuple of columns as defined in `CREATE
            TABLE`. The blob column with the pickled arguments is the
            last column.
            """
            args, kwargs = pickle.loads(row[-1])
            data = {
                key: row[i] for i, key in enumerate(
                    TASK_COLUMN_SEQUENCE.strip().split(",")[:-1]
                )
            }
            data["args"] = args
            data["kwargs"] = kwargs
            return HybridNamespace(data)

        return [process(row) for row in cursor.fetchall()]

    # pylint: disable=too-many-arguments
    def register_callable(
        self,
        func,
        uuid="",
        schedule=None,
        status=TASK_STATUS_WAITING,
        crontab="",
        args=None,
        kwargs=None,
        unique=False,
    ):
        """
        Store a callable in the task-table of the database. If the
        argument `unique` is given, don't register a callable twice. In
        this case an already registered callable with the same
        signature (module.name) gets overwritten. This can be useful for
        cron-tasks to not register them multiple times.
        """
        if self.is_initialized:
            if unique:
                tasks = self.get_tasks_by_signature(func)
                for task in tasks:
                    self.delete_callable(task)
            if not schedule:
                schedule = datetime.datetime.now()
            if args is None:
                args = ()
            if kwargs is None:
                kwargs = {}
            arguments = pickle.dumps((args, kwargs))
            data = {
                "uuid": uuid,
                "schedule": schedule,
                "status": status,
                "crontab": crontab,
                "function_module": func.__module__,
                "function_name": func.__name__,
                "function_arguments": arguments,
            }
            self._execute(CMD_STORE_TASK, data)
            if uuid:
                self.register_result(func, uuid, args=args, kwargs=kwargs)
        else:
            self._preregister_task(locals())

    def get_tasks(self):
        """
        Generic method to return all tasks as a list of HybridNamespace
        instances.
        """
        cursor = self._execute(CMD_GET_TASKS)
        return self._fetch_all_callable_entries(cursor)

    def get_tasks_on_due(self, schedule=None, status=None, new_status=None):
        """
        Returns tasks on due as a list of HybridNamespace instances. If
        'status' is given the status is also used for the selection. If
        'new_status' is given the selection gets updated with the new
        status.
        """
        if not schedule:
            schedule = datetime.datetime.now()
        if status:
            cursor = self._execute(
                CMD_GET_TASKS_ON_DUE_WITH_STATUS,
                [schedule, status]
            )
        else:
            cursor = self._execute(CMD_GET_TASKS_ON_DUE, [schedule])
        tasks = self._fetch_all_callable_entries(cursor)
        if new_status and tasks:
            values = [(new_status, task.rowid) for task in tasks]
            self._execute(CMD_UPDATE_TASK_STATUS, values, many=True)
            # also update the status in the previous fetched tasks:
            for task in tasks:
                task.status = new_status
        return tasks

    def get_tasks_by_signature(self, func):
        """
        Return all tasks matching the function-signature as a list of
        HybridNamespace instances.
        """
        parameters = func.__module__, func.__name__
        cursor = self._execute(CMD_GET_TASKS_BY_NAME, parameters)
        return self._fetch_all_callable_entries(cursor)

    def delete_callable(self, entry):
        """
        Delete the entry in the callable-table. Entry should be a
        dictionary as returned from `get_tasks_on_due()`. The row to delete
        gets identified by the `rowid`.
        """
        self._execute(CMD_DELETE_TASK, [entry["rowid"]])

    def get_crontasks(self):
        """
        Return all crontasks as a list of HybridNamespace instances.
        """
        cursor = self._execute(CMD_GET_CRONTASKS)
        return self._fetch_all_callable_entries(cursor)

    def delete_cronjobs(self):
        """
        Delete all cronjobs from the task-table.
        """
        self._execute(CMD_DELETE_CRON_TASKS)

    def update_crontask_schedule(self, rowid, schedule):
        """
        Update the `schedule` of the table entry with the given `rowid`.
        As this should be a crontask the status is set to WAITING.
        """
        parameters = schedule, TASK_STATUS_WAITING, rowid
        self._execute(CMD_UPDATE_CRONTASK_SCHEDULE, parameters)

    def count_tasks(self):
        """
        Returns the number of rows in the task-table, therefore
        providing the number of tasks stored in the database.
        """
        return self._count_table_rows(DB_TABLE_NAME_TASK)


    # -- result-methods ---

    @property
    def result_ttl(self):
        """
        Returns the new ttl as a datetime instance with an offset of now.
        """
        return datetime.datetime.now() + self._result_ttl

    def register_result(
            self,
            func,
            uuid,
            args=(),
            status=TASK_STATUS_WAITING,
            kwargs=None,
        ):
        """
        Register an entry in the result table of the database. The entry
        stores the uuid and the status `False` as zero `0` because the
        task is pending and no result available jet.
        """
        if not kwargs:
            kwargs = {}
        arguments = pickle.dumps((args, kwargs))
        data = {
            "uuid": uuid,
            "status": status,
            "function_module": func.__module__,
            "function_name": func.__name__,
            "function_arguments": arguments,
            "function_result": pickle.dumps(None),
            "error_message": "",
            "ttl": self.result_ttl,
        }
        self._execute(CMD_STORE_RESULT, data)

    def get_results(self):
        """
        Generic method to return all results as a list of TaskResult
        instances.
        """
        cursor = self._execute(CMD_GET_RESULTS)
        rows = cursor.fetchall()
        return [TaskResult.from_data_tuple(row) for row in rows]

    def get_result_by_uuid(self, uuid):
        """
        Return a dataset (as TaskResult) or None.
        """
        cursor = self._execute(CMD_GET_RESULT_BY_UUID, (uuid,))
        row = cursor.fetchone()  # tuple of data or None
        if row:
            result = TaskResult.from_data_tuple(row)
        else:
            result = None
        return result

    def update_result(self, uuid, result=None, error_message=""):
        """
        Updates the result-entry with the given `uuid` to status 1|2 and
        stores the `result` or `error_message`.
        """
        status = TASK_STATUS_ERROR if error_message else TASK_STATUS_READY
        function_result = pickle.dumps(result)
        ttl = self.result_ttl
        parameters = status, function_result, error_message, ttl, uuid
        self._execute(CMD_UPDATE_RESULT, parameters)

    def count_results(self):
        """
        Returns the number of rows in the task-table, therefore
        providing the number of tasks stored in the database.
        """
        return self._count_table_rows(DB_TABLE_NAME_RESULT)

    def delete_outdated_results(self):
        """
        Deletes results with status TASK_STATUS_READY that have exceeded
        the time to live (ttl).
        """
        now = datetime.datetime.now()
        self._execute(CMD_DELETE_OUTDATED_RESULTS, (now,))

    # -- setting-methods ---

    def get_settings(self):
        """
        Returns a HybridNamespace instance with the settings as attributes:
        - max_workers
        - running_workers
        - monitor_lock
        - autocron_lock
        - monitor_idle_time
        - worker_idle_time
        - worker_pids
        - result_ttl
        - rowid (not a setting but included)
        """
        cursor = self._execute(CMD_SETTINGS_GET_SETTINGS)
        row = cursor.fetchone()  # there is only one row
        col_names = SETTINGS_COLUMN_SEQUENCE.split(",")
        data = dict(zip(col_names, row))
        for key in BOOLEAN_SETTINGS:
            data[key] = bool(data[key])
        return HybridNamespace(data)

    def set_settings(self, settings):
        """
        Takes a HybridNamespace instance as settings
        argument (like the one returned from get_settings) and updates
        the setting values in the database.
        """
        data = (
            settings.max_workers,
            settings.running_workers,
            int(settings.monitor_lock),
            int(settings.autocron_lock),
            settings.monitor_idle_time,
            settings.worker_idle_time,
            settings.worker_pids,
            settings.result_ttl,
            settings.rowid
        )
        self._execute(CMD_SETTINGS_UPDATE, data)

    def get_monitor_idle_time(self):
        """
        Convenience function to get the monitor_idle_time from the settings.
        """
        return self.get_settings().monitor_idle_time

    def get_worker_idle_time(self):
        """
        Convenience function to get the worker_idle_time from the settings.
        """
        return self.get_settings().worker_idle_time

    def get_max_workers(self):
        """
        Convenience function to get the max_workers from the settings.
        """
        return self.get_settings().max_workers

    @property
    def monitor_lock_flag_is_set(self):
        """
        Returns the status of the monitor-lock flag..
        """
        return self.get_settings().monitor_lock

    def set_monitor_lock_flag(self, value):
        """
        Set monitor_lock flag to the given state.
        """
        settings = self.get_settings()
        settings.monitor_lock = value
        self.set_settings(settings)

    def increment_running_workers(self, pid):
        """
        Increment the running_worker-setting by 1.
        """
        settings = self.get_settings()
        if settings.worker_pids:
            pids = settings.worker_pids.split(",")
        else:
            pids = []
        pids.append(str(pid))
        settings.worker_pids = ",".join(pids)
        settings.running_workers += 1
        self.set_settings(settings)

    def decrement_running_workers(self, pid):
        """
        Decrement the running_worker-setting by 1.
        But don't allow a value below zero.
        """
        settings = self.get_settings()
        if settings.worker_pids:
            pids = settings.worker_pids.split(",")
        else:
            pids = []
        try:
            pids.remove(str(pid))
        except ValueError:
            # can happen when decrement gets called before increment
            # otherwise it is a weird error that should not happen
            pass
        else:
            if settings.running_workers > 0:
                settings.running_workers -= 1
            settings.worker_pids = ",".join(pids)
            self.set_settings(settings)
