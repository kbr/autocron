"""
admin.py

Administration tool to access the database.
"""

# this module is part of autocron
# copyright (c) 2024 Klaus Bremer
# all rights reserved
#
# license: MIT

import argparse
import shutil

from autocron.sqlite_interface import (
    SQLiteInterface,
    SETTINGS_DEFAULT_DATA,
)

interface = SQLiteInterface()


PGM_NAME = "autocron command line tool"
PGM_DESCRIPTION = """
    Provides access to the autocron database to change settings
    an get information about the running workers.
"""


class Admin:
    """Collection of methods for administration-tasks."""

    def __init__(self, db_name, initialize_db=True):
        self.interface = SQLiteInterface()
        if initialize_db:
            self.interface.init_database(db_name)
        else:
            self.interface.db_name = db_name

    def show_info(self):
        """Tabular view of the settings."""
        settings = self.interface.get_settings()
        column_width = len(max(settings.columns, key=len))
        database = "database"
        print(f"\n{database:<{column_width}}: '{self.interface.db_name}'")
        print(settings)
        tasks = self.interface.count_tasks()
        results = self.interface.count_results()
        for name, value in zip(("tasks", "results"), (tasks, results)):
            print(f"{name:<{column_width}}: {value}")
        print()

    def show_pending_tasks(self):
        """Tabular view of pending tasks."""
        header = "schedule"
        header += " " * (20 - len(header))
        header += "task"
        print(f"\n{header}")
        columns, _ = shutil.get_terminal_size()
        line = "-" * columns
        print(line)
        tasks = self.interface.get_tasks()
        for task in tasks:
            print(task)
        print()

    def show_results(self):
        """view the results"""
        print("\nresults")
        columns, _ = shutil.get_terminal_size()
        line = "-" * columns
        print(line)
        results = self.interface.get_results()
        for result in results:
            print(result)
        print()

    def set_max_workers(self, workers):
        """Set the number of workers."""
        settings = self.interface.get_settings()
        settings.max_workers = workers
        self.interface.update_settings(settings)
        print(f"set max_workers to {workers}")

    def set_autocron_lock(self, flag):
        """Set the autocron_lock flag."""
        settings = self.interface.get_settings()
        settings.autocron_lock = convert_flag(flag)
        self.interface.update_settings(settings)
        print(f"set autocron lock to {flag}")

    def set_monitor_lock(self, flag):
        """Set the monitor_lock flag."""
        settings = self.interface.get_settings()
        settings.monitor_lock = convert_flag(flag)
        self.interface.update_settings(settings)
        print(f"set monitor lock to {flag}")

    def set_blocking_mode(self, flag):
        """Set the blocking_mode flag."""
        settings = self.interface.get_settings()
        settings.blocking_mode = convert_flag(flag)
        self.interface.update_settings(settings)
        print(f"set blocking mode to {flag}")

    def set_worker_idle_time(self, idle_time):
        """
        Set the idle time of the worker in seconds. This is the time the
        worker sleeps when no new tasks are on due.
        """
        settings = self.interface.get_settings()
        settings.worker_idle_time = idle_time
        self.interface.update_settings(settings)
        print(f"set worker idle time to {idle_time} seconds")

    def set_monitor_idle_time(self, idle_time):
        """
        Set the idle time of the worker in seconds. This is the time the
        worker sleeps when no new tasks are on due.
        """
        settings = self.interface.get_settings()
        settings.monitor_idle_time = idle_time
        self.interface.update_settings(settings)
        print(f"set monitor idle time to {idle_time} seconds")

    def set_result_ttl(self, ttl):
        """
        Set the result time to life in seconds. ttl is an integer. This is
        the timespan a result will get stored in the database.
        """
        settings = self.interface.get_settings()
        settings.result_ttl = ttl
        self.interface.update_settings(settings)
        print(f"Set result-ttl to {ttl} seconds")

    def set_defaults(self):
        """Reset all settings to the default values."""
        settings = self.interface.get_settings()
        settings.__dict__.update(SETTINGS_DEFAULT_DATA)
        self.interface.update_settings(settings)
        print("\nautocron reset to default data:")
        self.show_info()

    def delete_database(self):
        """
        Delete the sqlite-database. A new one will get created on the
        next start of the admin-tool or autocron.
        """
        answer = input("sure to delete the current database? [y/n]: ")
        if answer.lower() == "y":
            self.interface.db_name.unlink(missing_ok=True)
            self.interface.db_name = None
        else:
            print("abort command")


def convert_flag(flag):
    """
    flag can be a string with 'true'', 'false' or 'on'', 'off''.
    Returns a boolean.
    """
    flag = flag.lower()
    return flag in {"true", "on"}


def print_usage():
    """Print program-description and hint how to get help."""
    print(f"\n{PGM_NAME}\n{PGM_DESCRIPTION}\nuse option -h for help.\n\n")


def get_command_line_arguments():
    """Get the command line arguments."""
    parser = argparse.ArgumentParser(
        prog=PGM_NAME,
        description=PGM_DESCRIPTION,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("database", help="database name or path.")
    parser.add_argument(
        "-i",
        "--info",
        action="store_true",
        help="provide information about the settings and process data.",
    )
    parser.add_argument(
        "-t",
        "--tasks",
        action="store_true",
        help="view all pending tasks",
    )
    parser.add_argument(
        "-r",
        "--results",
        action="store_true",
        help="view stored results",
    )
    parser.add_argument(
        "--set-max-workers",
        dest="max_workers",
        type=int,
        help="set number of worker processes.",
    )
    parser.add_argument(
        "--set-autocron-lock",
        dest="autocron_lock",
        help="set autocron lock flag: [true|false or on|off].",
    )
    parser.add_argument(
        "--set-monitor-lock",
        dest="monitor_lock",
        help="set monitor lock flag: [true|false or on|off].",
    )
    parser.add_argument(
        "--set-blocking-mode",
        dest="blocking_mode",
        help="set blocking mode flag: [true|false or on|off].",
    )
    parser.add_argument(
        "--set-worker-idle-time",
        dest="worker_idle_time",
        type=int,
        help="set worker idle time in seconds [set to 0 for auto-calculation].",
    )
    parser.add_argument(
        "--set-monitor-idle-time",
        dest="monitor_idle_time",
        type=int,
        help="set monitor idle time in seconds.",
    )
    parser.add_argument(
        "--set-result-ttl",
        dest="result_ttl",
        type=int,
        help="set result time to life in seconds.",
    )
    parser.add_argument(
        "--set-defaults",
        dest="set_defaults",
        action="store_true",
        help="reset to default settings.",
    )
    parser.add_argument(
        "--delete-database",
        dest="delete_database",
        action="store_true",
        help="delete the current database.",
    )
    return parser.parse_args()


def main():
    """entry point."""
    args = get_command_line_arguments()
    initialize_db = not args.delete_database
    admin = Admin(args.database, initialize_db)
    if args.info:
        admin.show_info()
    elif args.tasks:
        admin.show_pending_tasks()
    elif args.results:
        admin.show_results()
    elif args.max_workers:
        admin.set_max_workers(args.max_workers)
    elif args.autocron_lock:
        admin.set_autocron_lock(args.autocron_lock)
    elif args.monitor_lock:
        admin.set_monitor_lock(args.monitor_lock)
    elif args.blocking_mode:
        admin.set_blocking_mode(args.blocking_mode)
    elif args.worker_idle_time:
        admin.set_worker_idle_time(args.worker_idle_time)
    elif args.monitor_idle_time:
        admin.set_monitor_idle_time(args.monitor_idle_time)
    elif args.result_ttl:
        admin.set_result_ttl(args.result_ttl)
    elif args.set_defaults:
        admin.set_defaults()
    elif args.delete_database:
        admin.delete_database()
    else:
        print_usage()


if __name__ == "__main__":
    main()
