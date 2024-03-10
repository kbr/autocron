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
import queue
import sqlite3
import threading
import time
import types


DEFAULT_STORAGE = ".autocron"
WRITE_THREAD_TIMEOUT = 2.0
SQLITE_MAX_RETRY_LIMIT = 10
SQLITE_OPERATIONAL_ERROR_DELAY = 0.01


# Status codes used for task-status the result-entries:
TASK_STATUS_WAITING = 1
TASK_STATUS_PROCESSING = 2
TASK_STATUS_READY = 3
TASK_STATUS_ERROR = 4
TASK_STATUS_UNAVAILABLE = 5


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
CMD_GET_NEXT_TASK =\
     CMD_GET_TASKS_ON_DUE + f" AND status == {TASK_STATUS_WAITING}"
CMD_GET_NEXT_CRONTASK=\
    CMD_GET_NEXT_TASK + " AND crontab <> ''"
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
    SELECT {RESULT_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_RESULT}
    WHERE status == {TASK_STATUS_READY}"""
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
DEFAULT_RESULT_TTL = 1800  # Storage time (time to live) for results in seconds

DEFAULT_DATA = {
    "max_workers": DEFAULT_MAX_WORKERS,
    "running_workers": DEFAULT_RUNNING_WORKERS,
    "monitor_lock": DEFAULT_MONITOR_LOCK,
    "autocron_lock": DEFAULT_AUTOCRON_LOCK,
    "monitor_idle_time": DEFAULT_MONITOR_IDLE_TIME,
    "worker_idle_time": DEFAULT_WORKER_IDLE_TIME,
    "worker_pids": DEFAULT_WORKER_PIDS,
    "result_ttl": DEFAULT_RESULT_TTL
}

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


def sqlite_call_wrapper(
    function,
    parameters=(),
    retries=SQLITE_MAX_RETRY_LIMIT,
    delay=SQLITE_OPERATIONAL_ERROR_DELAY
):
    """
    Helper function as wrapper for sqlite actions that may fail, i.e.
    because of a database lock. These functions are most often called
    from a worker to update status or results or by the decorator for
    registering. Therefor the blocking operation and sleep time does not
    affect the application that should not block.
    """
    message = ""
    for _ in range(retries):
        try:
            return function(*parameters)
        except sqlite3.OperationalError as err:
            message = str(err)
            time.sleep(delay)
    raise sqlite3.OperationalError(message)


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
        data = data if data else {}
        super().__init__(**data)

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
        return self.status == TASK_STATUS_WAITING

    @property
    def is_ready(self):
        """indicates task has been processed."""
        return self.status == TASK_STATUS_READY

    @property
    def has_error(self):
        """indicates error_message is set."""
        return self.status == TASK_STATUS_ERROR

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


# -------------------------------------
# database access and helpers

class Executor:
    """
    SQLite execution wrapper that makes a commit after commands changing
    the database-content and closes the connection after all work is
    done.

    usage:

        with Executor(dbname) as executor:
            cursor = executor.run(command, parameters)
            rows = corsor.fetchall()
            return rows

    `run()` can get called as often as required. The database keeps
    connected. Leaving the context will close the database connection.
    """

    def __init__(self, db_name, row_factory=None):
        self.row_factory = row_factory
        self.db_name = db_name
        self.connection = None

    def __enter__(self):
        self.connection = sqlite3.connect(
            self.db_name,
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        if self.row_factory:
            self.connection.row_factory = self.row_factory
        return self

    def __exit__(self, *args):
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

        Returns the result of the given command and causes a commit()
        """
        if parameters and many is None:
            # try to find out whether it is many or not:
            if isinstance(parameters, (tuple, list)):
                # could be qmark parameters or a sequence of tuples or dicts
                # take the first element of the sequence, if this is another
                # sequence or dict then these are parameters for an
                # executemany() command, else just execute()
                many = isinstance(parameters[0], (tuple, list, dict))
        with self.connection:
            if many:
                return self.connection.executemany(command, parameters)
            return self.connection.execute(command, parameters)


def task_row_factory(cursor, row):
    """
    SQLite factory class to convert a row from a task-table to a
    correponding HybridNamespace object.
    """
    function_arguments_column_name = TASK_COLUMN_SEQUENCE.rsplit(
        ",", maxsplit=1)[-1]
    data = {}
    column_names = [entry[0] for entry in cursor.description]
    for name, value in zip(column_names, row):
        if name == function_arguments_column_name:
            args, kwargs = pickle.loads(value)
            data["args"] = args
            data["kwargs"] = kwargs
        else:
            data[name] = value
    return HybridNamespace(data)


def result_row_factory(cursor, row):
    """
    SQLite factory class to convert a row from the result-table to a
    TaskResult instance.
    """
    column_names = [entry[0] for entry in cursor.description]
    data = dict(zip(column_names, row))
    result = TaskResult(data)
    result.function_result = pickle.loads(result.function_result)
    result.function_arguments = pickle.loads(result.function_arguments)
    return result


def settings_row_factory(cursor, row):
    """
    SQLite factory class to convert a row from a settings-table to a
    correponding HybridNamespace object.
    """
    column_names = [entry[0] for entry in cursor.description]
    data = {name: bool(value) if name in BOOLEAN_SETTINGS else value
            for name, value in zip(column_names, row)}
    return HybridNamespace(data)


def _execute_sqlite_command(db_name, command, parameters=(), many=False):
    """
    Run a command with parameters. Parameters can be a sequence of
    values to get used in an ordered way or a dictionary with
    key-value pairs, where the keys are the value-names used in the
    db (i.e. the column names).

    If 'many' is true then conn.executemany() gets called and
    parameters is interpreted differently as as sequence of ordered
    tuples or dictionaries as placeholders for the provided command.

    This is a blocking operation: the sqlite library will lock the
    database per process when writing to the database and each process
    will wait for the lock to be released to get their turn.
    """
    conn = sqlite3.connect(
        db_name,
        detect_types=sqlite3.PARSE_DECLTYPES
    )
    with conn:
        if many:
            return conn.executemany(command, parameters)
        return conn.execute(command, parameters)


def run_write_thread(exit_event, db_name, command_queue):
    """
    Start the write thread to delegate the blocking I/O out of the main
    process, that may run by an async framework.
    """
    while True:
        try:
            item = command_queue.get(timeout=WRITE_THREAD_TIMEOUT)
        except queue.Empty:
            # check for exit_event on empty queue so the queue items
            # can get handled before terminating the thread
            if exit_event.is_set():
                break
        else:
            # item is a sequence of arguments
            cmd, parameters, many = item
            _execute_sqlite_command(db_name, cmd, parameters, many)


class SQLiteInterface:
    """
    SQLite interface for application specific operations.
    This class is a Singleton.
    """
    # (this is not a god-class but a bit bigger than pylint likes it)
    # pylint: disable=too-many-public-methods
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self._preregistered_tasks = []
        self._result_ttl = datetime.timedelta(seconds=DEFAULT_RESULT_TTL)
        self._accept_registrations = True
        self._db_name = None
        self.autocron_lock_is_set = None
        self.command_queue = None
        self.write_thread = None
        self.exit_event = None

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

    @property
    def result_ttl(self):
        """
        Returns the new ttl as a datetime instance with an offset of now.
        """
        return datetime.datetime.now() + self._result_ttl

    def _preregister_task(self, data):
        """
        Take the data, which is a dictionary,and convert it to another
        dict without the key 'self'. Pickle the dict and append it to
        _preregistered tasks.
        """
        data = {k: v for k, v in data.items() if k != "self"}
        self._preregistered_tasks.append(data)

    def init_database(self, db_name):
        """
        Callable for delayed initialization. Set the database
        (which is a string or Path) and runs the correponding
        initialization.
        """
        # don't run init_database multiple times
        if not self.is_initialized:
            self.db_name = db_name

            with Executor(self.db_name) as sql:
                # create tables (if not existing)
                sql.run(CMD_CREATE_TASK_TABLE)
                sql.run(CMD_CREATE_RESULT_TABLE)
                sql.run(CMD_CREATE_SETTINGS_TABLE)

                # check for existing entries in the settings
                # if there are no entries initialize the settings
                # with the default values
                cmd = CMD_COUNT_TABLE_ROWS.format(
                    table_name=DB_TABLE_NAME_SETTINGS)
                cursor = sql.run(cmd)
                rows = cursor.fetchone()[0]
                if not rows:
                    sql.run(CMD_SETTINGS_STORE_VALUES, DEFAULT_DATA)

                # all task still there from the last run
                # are reset to TASK_STATUS_WAITING status:
                cursor = sql.run(CMD_GET_TASKS)
                cursor.row_factory = task_row_factory
                parameters = [(TASK_STATUS_WAITING, task.rowid)
                              for task in cursor.fetchall()]
                if parameters:
                    sql.run(CMD_UPDATE_TASK_STATUS, parameters)

            # register all callables processed at import time
            # before autocron started (should only be cron-tasks)
            for data in self._preregistered_tasks:
                self.register_task(**data)

            # read some of the current settings
            settings = self.get_settings()
            self.autocron_lock_is_set = settings.autocron_lock
            self._result_ttl = datetime.timedelta(seconds=settings.result_ttl)


    # -- write_thread-methods ---

    def start_write_thread(self):
        """
        Starts a thread to handle blocking I/O writing to the database.
        """
        # safety check for not starting the thread multiple times
        if not self.write_thread:
            self.command_queue = queue.Queue()
            self.exit_event = threading.Event()
            self.write_thread = threading.Thread(
                target=run_write_thread,
                args=(self.exit_event, self._db_name, self.command_queue)
            )
            self.write_thread.start()

    def stop_write_thread(self):
        """
        Terminates a running write_thread.
        """
        if self.write_thread:
            if self.exit_event:
                self.exit_event.set()
                self.write_thread = None


    # -- database api ---

    def get_row_num(self, table_name):
        """
        Return the number of entries in the given table.
        """
        cmd = CMD_COUNT_TABLE_ROWS.format(table_name=table_name)
        with Executor(self.db_name) as sql:
            cursor = sql.run(cmd)
            return cursor.fetchone()[0]

    def count_tasks(self):
        """
        Returns the number of rows in the task-table, therefore
        providing the number of tasks stored in the database.
        """
        return self.get_row_num(DB_TABLE_NAME_TASK)

    def count_results(self):
        """
        Returns the number of rows in the task-table, therefore
        providing the number of tasks stored in the database.
        """
        return self.get_row_num(DB_TABLE_NAME_RESULT)

    def register_task(self, func, schedule=None, crontab="", uuid="",
                      args=None, kwargs=None, unique=False):
        """
        Store a callable in the task-table of the database. If the
        callable is a delayed task with a potential result create also a
        corresponding entry in the result table.
        """
        if not self.is_initialized:
            self._preregister_task(locals())
        else:
            if not schedule:
                schedule = datetime.datetime.now()
            if args is None:
                args = ()
            if kwargs is None:
                kwargs = {}
            arguments = pickle.dumps((args, kwargs))
            task_data = {
                "uuid": uuid,
                "schedule": schedule,
                "status": TASK_STATUS_WAITING,
                "crontab": crontab,
                "function_module": func.__module__,
                "function_name": func.__name__,
                "function_arguments": arguments,
            }

            with Executor(self._db_name, row_factory=task_row_factory) as sql:
                if unique:
                    parameters = (func.__module__, func.__name__)
                    cursor = sql.run(CMD_GET_TASKS_BY_NAME, parameters)
                    for task in cursor.fetchall():
                        sql.run(CMD_DELETE_TASK, [task.rowid])
                sql.run(CMD_STORE_TASK, task_data)
                # a delayed task has a uuid: create a result entry
                if uuid:
                    result_data = {
                        "uuid": uuid,
                        "status": TASK_STATUS_WAITING,
                        "function_module": func.__module__,
                        "function_name": func.__name__,
                        "function_arguments": arguments,
                        "function_result": pickle.dumps(None),
                        "error_message": "",
                        "ttl": self.result_ttl,
                    }
                    sql.run(CMD_STORE_RESULT, result_data)

    def get_tasks(self):
        """
        Generic method to return all tasks as a list of HybridNamespace
        instances.
        """
        with Executor(self._db_name, row_factory=task_row_factory) as sql:
            cursor = sql.run(CMD_GET_TASKS)
            return cursor.fetchall()

    def get_next_task(self, prefer_cron=True):
        """
        Returns the next task on due in waiting state or None. If
        `prefer_cron` is True check for cron-tasks on due first. If
        `prefer_cron` is False any task on due may get returned
        (including cron). If no task is on due return None. If a task is
        returned, the status is set to TASK_STATUS_PROCESSING (and also
        updated in the database).
        """
        commands = []
        if prefer_cron:
            commands.append(CMD_GET_NEXT_CRONTASK)
        commands.append(CMD_GET_NEXT_TASK)
        parameters = (datetime.datetime.now(),)

        with Executor(self.db_name, row_factory=task_row_factory) as sql:
            for command in commands:
                cursor = sql.run(command, parameters)
                task = cursor.fetchone()
                if task:
                    task.status = TASK_STATUS_PROCESSING
                    sql.run(CMD_UPDATE_TASK_STATUS, (task.status, task.rowid))
                    break
        return task

    def get_tasks_on_due(self, schedule=None, status=None, new_status=None):
        """
        Returns tasks on due as a list of HybridNamespace instances. If
        'status' is given the status is also used for the selection. If
        'new_status' is given the selection gets updated with the new
        status.
        """
        # Note: this method is for admin-use and not used otherwise
        if not schedule:
            schedule = datetime.datetime.now()
        if status:
            command = CMD_GET_TASKS_ON_DUE_WITH_STATUS
            parameters = (schedule, status)
        else:
            command = CMD_GET_TASKS_ON_DUE
            parameters = (schedule,)
        with Executor(self.db_name, row_factory=task_row_factory) as sql:
            cursor = sql.run(command, parameters=parameters)
            tasks = cursor.fetchall()
            if tasks and new_status:
                parameters = [(new_status, task.rowid) for task in tasks]
                sql.run(
                    CMD_UPDATE_TASK_STATUS, parameters=parameters, many=True
                )
        # also update the status in the previous fetched tasks:
        for task in tasks:
            task.status = new_status
        return tasks

    def delete_task(self, task):
        """
        Deletes the given task, which is a HybridNamespace object with
        """
        with Executor(self.db_name) as sql:
            sql.run(CMD_DELETE_TASK, (task.rowid,))

    def get_crontasks(self):
        """
        Return all crontasks as a list of HybridNamespace instances.
        """
        with Executor(self.db_name, row_factory=task_row_factory) as sql:
            cursor = sql.run(CMD_GET_CRONTASKS)
            return cursor.fetchall()

    def delete_crontasks(self):
        """
        Delete all crontasks from the task-table.
        """
        with Executor(self.db_name) as sql:
            sql.run(CMD_DELETE_CRON_TASKS)

    def update_task_schedule(self, task, schedule):
        """
        Update the schedule on a task. Usefull for crontasks.
        """
        with Executor(self.db_name) as sql:
            parameters = schedule, TASK_STATUS_WAITING, task.rowid
            sql.run(CMD_UPDATE_CRONTASK_SCHEDULE, parameters)

    def get_result_by_uuid(self, uuid):
        """
        Return a dataset (as TaskResult) or None.
        """
        with Executor(self.db_name, row_factory=result_row_factory) as sql:
            cursor = sql.run(CMD_GET_RESULT_BY_UUID, (uuid,))
            return cursor.fetchone()  # tuple of data or None

    def get_results(self):
        """
        Get of all results with status TASK_STATUS_READY as a list of
        TaskResult instances.
        """
        with Executor(self.db_name, row_factory=result_row_factory) as sql:
            cursor = sql.run(CMD_GET_RESULTS)
            return cursor.fetchall()

    def update_result(self, uuid, result=None, error_message=""):
        """
        Updates the result-entry with the given `uuid` to status 1|2 and
        stores the `result` or `error_message`.
        """
        status = TASK_STATUS_ERROR if error_message else TASK_STATUS_READY
        function_result = pickle.dumps(result)
        ttl = self.result_ttl
        parameters = (status, function_result, error_message, ttl, uuid)
        with Executor(self.db_name) as sql:
            sql.run(CMD_UPDATE_RESULT, parameters)

    def delete_outdated_results(self):
        """
        Deletes results with status TASK_STATUS_READY that have exceeded
        the time to live (ttl).
        """
        now = datetime.datetime.now()
        with Executor(self.db_name) as sql:
            sql.run(CMD_DELETE_OUTDATED_RESULTS, (now,))


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
        with Executor(self.db_name, row_factory=settings_row_factory) as sql:
            cursor = sql.run(CMD_SETTINGS_GET_SETTINGS)
            settings = cursor.fetchone()  # there is only one row
        return settings

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
        with Executor(self._db_name) as sql:
            sql.run(CMD_SETTINGS_UPDATE, data)

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
