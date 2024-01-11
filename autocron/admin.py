"""
admin.py

Administration tool to access the database.
"""

import argparse

from autocron.sql_interface import (
    SQLiteInterface,
    DEFAULT_MAX_WORKERS,
    DEFAULT_RUNNING_WORKERS,
    DEFAULT_MONITOR_LOCK,
    DEFAULT_AUTOCRON_LOCK,
)

interface = SQLiteInterface()


PGM_NAME = "autocron command line tool"
PGM_DESCRIPTION = """
    Allows access to the autocron database to change default
    settings and to inspect waiting tasks and stored results.
"""


def _format_item_list(data, separator="\n", divider=":", column_width=None):
    """
    Helperfunction: takes a string representing a list of data-pairs
    separated by a given separator, with the data in the data-pair
    separated by the given divider (i.e. "1:2\n3:4" etc). Formats the
    data pairs according to the longest first item, so that the dividers
    (the colons in the exaple data given above) are vertical aligned.
    Returns the new formatted string.
    """
    keys = []
    values = []
    for line in data.split(separator):
        key, value = line.split(divider, 1)
        keys.append(key)
        values.append(value)
    if column_width is None:
        # here is pylint wrong:
        # pylint: disable=unnecessary-lambda
        column_width = len(max(keys, key=lambda x: len(x)))
    template = f"{{:{column_width}}}{divider} {{}}"
    return separator.join(template.format(k, v) for k, v in zip(keys, values))


def report_info():
    """Report the settings and statistics about tasks and results."""
    settings = interface.get_settings()
    task_num = interface.count_tasks()
    result_num = interface.count_results()
    report = f"{settings}\ntasks:{task_num}\nresults:{result_num}"
    formated_report = _format_item_list(report)
    output = f"\nSettings\n{formated_report}\n"
    print(output)


def _report_tasks(tasks, task_type="tasks"):
    """
    Helperfunction: takes a list of Hybridnamespace intstances and print
    them nicely formated to stdout.
    """
    print(f"\n{task_type} found: {len(tasks)}")
    divider = "-" * 50
    for task in tasks:
        task = _format_item_list(str(task))
        print(f"{divider}\n{task}")
    print(divider)
    print()


def report_tasks():
    """Report all stored tasks."""
    _report_tasks(interface.get_tasks())


def report_tasks_on_due():
    """Report all tasks waiting for execution and are on due."""
    _report_tasks(interface.get_tasks_on_due())


def report_cron_tasks():
    """Report all task which are cron-jobs."""
    _report_tasks(interface.get_crontasks())


def report_results():
    """Report all available results."""
    results = interface.get_results()
    _report_tasks(results, task_type="results")


def print_usage():
    """Print program-description and hint how to get help."""
    print(f"\n{PGM_NAME}\n{PGM_DESCRIPTION}\nuse option -h for help.\n\n")


def reset_defaults():
    """Reset the settings with the default values."""
    settings = interface.get_settings()
    settings.max_workers = DEFAULT_MAX_WORKERS
    settings.running_workers = DEFAULT_RUNNING_WORKERS
    settings.monitor_lock = DEFAULT_MONITOR_LOCK
    settings.autocron_lock = DEFAULT_AUTOCRON_LOCK
    interface.set_settings(settings)
    print("\nReset settings default values:")
    report_info()


def set_max_workers(workers):
    """Change value for the number of allowed max_workers."""
    settings = interface.get_settings()
    settings.max_workers = workers
    interface.set_settings(settings)
    print(f"Set max_workers to {workers}")


def set_worker_idle_time(idle_time):
    """
    Set the idle time of the worker in seconds. This is the time the
    worker sleeps when no new tasks are on due.
    """
    settings = interface.get_settings()
    settings.worker_idle_time = idle_time
    interface.set_settings(settings)
    print(f"Set worker idle time to {idle_time} seconds")


def set_monitor_idle_time(idle_time):
    """
    Set the idle time of the worker in seconds. This is the time the
    worker sleeps when no new tasks are on due.
    """
    settings = interface.get_settings()
    settings.monitor_idle_time = idle_time
    interface.set_settings(settings)
    print(f"Set monitor idle time to {idle_time} seconds")


def _convert_flag(flag):
    """
    flag can be a string with 'true'', 'false' or 'on'', 'off''.
    Returns a boolean.
    """
    flag = flag.lower()
    return flag in {"true", "on"}


def set_monitor_lock(flag):
    """Set monitor_lock flag."""
    flag = _convert_flag(flag)
    interface.set_monitor_lock_flag(flag)
    print(f"Set monitor lock to {flag}")


def set_autocron_lock(flag):
    """Set autocron_lock flag."""
    flag = _convert_flag(flag)
    settings = interface.get_settings()
    settings.autocron_lock = flag
    interface.set_settings(settings)
    print(f"Set autocron lock to {flag}")


def delete_database():
    """
    Delete the database and create the database again with the default
    settings.
    """
    answer = input("Sure to delete the current database? [y/n]: ")
    if answer.lower() == 'y':
        interface.db_name.unlink()
        # this could be all, because at the next autocron import
        # a new database will get created.
        # However, create a new database here and now:
        # pylint: disable=protected-access
        interface._init_database()
    else:
        print("abort command")


def get_command_line_arguments():
    """Get the command line arguments."""
    parser = argparse.ArgumentParser(
        prog=PGM_NAME,
        description=PGM_DESCRIPTION,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "database",
        help="database name or path."
    )
    parser.add_argument(
        "-i", "--info",
        action="store_true",
        help="provide information about the settings, number of waiting tasks "
             "and result entries."
    )
    parser.add_argument(
        "--reset-defaults",
        dest="reset_defaults",
        action="store_true",
        help="restore the default settings: max_workers=1, running_workers=0, "
             "monitor_lock=0, autocron_lock=0."
    )
    parser.add_argument(
        "--delete-database",
        dest="delete_database",
        action="store_true",
        help="delete the current database and create a new clean one with "
             "the default settings."
    )
    parser.add_argument(
        "--set-max-workers",
        dest="max_workers",
        type=int,
        help="set number of maximum worker processes."
    )
    parser.add_argument(
        "--set-worker-idle-time",
        dest="worker_idle_time",
        type=float,
        help="set worker idle time in seconds."
    )
    parser.add_argument(
        "--set-monitor-idle-time",
        dest="monitor_idle_time",
        type=float,
        help="set monitor idle time in seconds."
    )
    parser.add_argument(
        "--set-monitor-lock",
        dest="monitor_lock",
        help="set monitor lock flag: [true|false or on|off]."
    )
    parser.add_argument(
        "--set-autocron-lock",
        dest="autocron_lock",
        help="set autocron lock flag: [true|false or on|off]."
    )
    parser.add_argument(
        "-t", "--get-tasks",
        dest="get_tasks",
        action="store_true",
        help="list all waiting tasks."
    )
    parser.add_argument(
        "-d", "--get-tasks-on-due",
        dest="get_tasks_on_due",
        action="store_true",
        help="list all waiting tasks that are on due."
    )
    parser.add_argument(
        "-c", "--get-cron-tasks",
        dest="get_cron_tasks",
        action="store_true",
        help="list all task which are cronjobs."
    )
    parser.add_argument(
        "-r", "--get-results",
        dest="get_results",
        action="store_true",
        help="list all available results."
    )
    return parser.parse_args()


def main(args=None):
    """entry point."""
    # pylint: disable=too-many-branches
    if not args:
        args = get_command_line_arguments()
    # like the worker just use the database "as is",
    # by not running the initialization.
    interface.db_name = args.database
    if args.info:
        report_info()
    elif args.reset_defaults:
        reset_defaults()
    elif args.max_workers:
        set_max_workers(args.max_workers)
    elif args.get_tasks:
        report_tasks()
    elif args.get_tasks_on_due:
        report_tasks_on_due()
    elif args.get_cron_tasks:
        report_cron_tasks()
    elif args.get_results:
        report_results()
    elif args.delete_database:
        delete_database()
    elif args.monitor_lock:
        set_monitor_lock(args.monitor_lock)
    elif args.autocron_lock:
        set_autocron_lock(args.autocron_lock)
    elif args.worker_idle_time:
        set_worker_idle_time(args.worker_idle_time)
    elif args.monitor_idle_time:
        set_worker_idle_time(args.monitor_idle_time)
    else:
        print_usage()


if __name__ == "__main__":
    main(get_command_line_arguments())
