
Admin Interface
===============

**autocron** comes with a command line tool to inspect the content of the database and to manage default settings. The flag ``-h`` provides help for usage: ::

    $ autocron -h
    usage: autocron command line tool [-h] [-i] [--reset-defaults]
    [--delete-database] [--set-max-workers MAX_WORKERS] [-t] [-d] [-c] [-r]

    Allows access to the autocron database to change default settings and to
    inspect waiting tasks and stored results.

    options:
      -h, --help            show this help message and exit
      -i, --info            provide information about the settings, number
                            of waiting tasks and result entries.
      --reset-defaults      restore the default settings: max_workers=1,
                            running_workers=0.
      --delete-database     delete the current database and create a new
                            clean one with the default settings.
      --set-max-workers MAX_WORKERS
                            set number of maximum worker processes.
      -t, --get-tasks       list all waiting tasks.
      -d, --get-tasks-on-due
                            list all waiting tasks that are on due.
      -c, --get-cron-tasks  list all task which are cronjobs.
      -r, --get-results     list all available results.
