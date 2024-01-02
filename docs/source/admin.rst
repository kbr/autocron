.. _admin-iterface:

Admin Interface
===============


**autocron** comes with a command line tool to inspect the content of the database and to manage default settings: ::

    $ autocron
    usage: autocron command line tool [-h] [-i] [--reset-defaults]
    [--delete-database] [--set-max-workers MAX_WORKERS]
    [--set-monitor-lock MONITOR_LOCK] [--set-autocron-lock
    AUTOCRON_LOCK] [-t] [-d] [-c] [-r] database



``database:``
    Name of the database to work on, required.

``-c, --get-cron-tasks:``
    list all registered task which are cronjobs.

``-d, --get-tasks-on-due:``
    list all waiting tasks that are on due.

``--delete-database:``
    deletes the selected database.

``-h:``
    show the help menu

``-i:``
    views the current the settings, number of aktive workers and their corresponding ``pids`` in case autocron is running.

``-r, --get-results:``
    list all available results.

``--reset-defaults:``
    overwrite the settings in the database with the default values.

``--set-autocron-lock:``
    set the lock flag of autocron. If the flag is set, autocron is disabled and will not start. This can be helpful on development or debugging when additional background-processes may be disturbing at best. Accepts the case-insensitive arguments ``on, off, true, false``. ``on`` and ``true`` are setting the flag (disable autocron), ``off`` and ``false`` delete the flag (enable autocron):

``--set-max-workers:``
    set number of maximum worker processes. Takes an integer as argument. (autocron will start exactly this number of workers and not "as up to".) To take effect autocron needs to restart.

``--set-monitor-lock:``
    set monitor lock flag. This is an internal flag indicating that a monitor process is active. When autocron starts with multiple processes of the web-application in parallel, the first process will start the monitor and worker processes. Setting the flag prevents the other processes to do the same and start additonal workers on their own. On shutdown, this flag gets released. However the admin tool allows to set the flag if something does not work as it should. Arguments are ``on, off, true, false``. During normal operation you should never have the need to deal with this setting.

``-t, --get-tasks:``
    list all waiting tasks.


Practical Settings
------------------

By default autocron starts with one (1) **worker process**. When no tasks are on due the worker sleeps for a given idle-time. When waking up the worker selects all tasks on due for processing. After handling these tasks the worker looks for new tasks that may have been registered meanwhile. If no tasks have been registered the worker goes into idle mode again. A second running worker may select tasks for processing that are registered during the time the first worker is busy. More than one workers make sense if tasks are registered in a faster pace than a single worker can process them. With the ``--set-max-workers`` setting the number of workers can get set accordingly. To take effect, autocron needs a restart. As autocron gets started from the application the application has to restart. There is no upper limit for the number of workers, but it makes no sense to set the number too high – even not for "reserves" or to be on the "safe side". Best advice is to check the server- and process-loads to find the best settings. The option ``-i`` lists the ``pids`` of the worker processes for inspection.

For **debugging** an application, active background processes can be disturbing. autocron can get deactivated with the ``--set-autocron-lock`` setting. The default value is ``False``. Set the value to ``True`` or ``on`` to set the flag to deactivate autocron (the arguments are case insensitive). Even when ``autocron.start()`` is called in the code, autocron will check the flag and will not start. The ``delay`` decorator will behave the same returning a ``TaskResult`` instance but the task itself will get executed in ``sync``, i.e. a blocking task will block for some time.


Database Storage
----------------

All settings are stored in the same **database** as the tasks to process by the workers. The default location for the databases is in the ``~.autocron/`` directory. When autocron starts and can not find its database, a new database gets created with all settings set to the default values. During compile time of the application autocron may already preregister ``cron`` decorated functions for later execution. The registration will be completed with the call of ``autocron.start()`` and the calls of ``delay`` decorated functions will register the corresponding functions for later execution in the database at the application runtime. On shutting down the application all unhandled tasks in the autocron database survive and will get handled the next time the application starts. If the database file still holding tasks gets deleted, the tasks are lost and will not executed on the next start of the application. Otherwise it is safe to delete the databases – next time when the application starts autocron will create a fresh database.




