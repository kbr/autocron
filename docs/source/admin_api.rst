.. _admin-iterface:

Admin Interface
===============


**autocron** comes with a command line tool to inspect the content of the database and to manage the settings: ::


    $ autocron
    usage: autocron command line tool [-h] [-i]
    [--set-max-workers MAX_WORKERS] [--set-autocron-lock AUTOCRON_LOCK]
    [--set-monitor-lock MONITOR_LOCK] [--set-worker-idle-time WORKER_IDLE_TIME]
    [--set-monitor-idle-time MONITOR_IDLE_TIME]
    [--set-result-ttl RESULT_TTL]
    [--set-defaults] [--delete-database]
    database


``database:``
    Name of the database to work on, required.

``--delete-database:``
    deletes the selected database.

``-h:``
    show the help menu

``-i:``
    views the current the settings, number of aktive workers and their corresponding ``pids`` in case autocron is running.

``--set-defaults:``
    set the database to the default settings.

``--set-autocron-lock:``
    set the lock flag of autocron. If the flag is set, autocron is disabled and will not start. Accepts the case-insensitive arguments ``on, off, true, false``. ``on`` and ``true`` are setting the flag (disable autocron), ``off`` and ``false`` delete the flag (enable autocron):

``--set-max-workers:``
    set number of maximum worker processes. Takes an integer as argument. (autocron will start exactly this number of workers and not "as up to".) To take effect autocron needs to restart. A usefull number of workers is up to the number of cores. Defaults to 1.

``--set-monitor-idle-time:``
    set the idle time (integer in seconds) of the monitor thread supervising the workers. Default value is 5 seconds. Normalwise there is no need to change this.

``--set-monitor-lock:``
    set monitor lock flag. This is an internal flag indicating that a monitor process is active. When autocron starts with multiple processes of the web-application in parallel, the first process will start the monitor and the worker processes. Setting the flag prevents the other processes to do the same and start additonal workers on their own. On shutdown, this flag gets released. However the admin tool allows to set the flag if something does not work as it should. Arguments are ``on, off, true, false``. During normal operation you should never have the need to deal with this setting.

``--set-worker-idle-time:``
    set the idle time (integer in seconds) for the worker processes. If no tasks on due the worker(s) will sleep for the given time before checking again for new tasks. Defaults to 0 seconds, what means that the value is auto-calculated. Normalwise there is no need to change this.

``--set-result-ttl:``
    set the result time to life (ttl, in seconds). Stored results are deleted after this timespan. A change to take effect needs a restart of the application. The change will applied to new results. Already stored results keep the previous assigned ttl. Defauts to 1800 seconds (30 minutes).


Settings
--------

**autocron** comes with default settings. The admin interface allows to change the settings. By default autocron starts with one (1) **worker process**. When no tasks are on due the worker sleeps for 1 second. When waking up the worker selects the next tasks on due for processing. After handling the task the worker looks for a next task that may have been registered meanwhile. If no tasks have been registered the worker goes into idle mode again. The duration of the idle-time can be set with the option ``--set-worker-idle-time``.

More workers to handle tasks are usefull if tasks are registered in a faster pace than a single worker can process them. There is no upper limit for the number of workers, but normalwise it makes no sense to start more workers than the number of available cpu-cores.

**autocron** can get deactivated with the ``--set-autocron-lock`` setting. This can be usefull for **debugging**. The default value is ``False``. Set the value to ``True`` or ``on`` to deactivate autocron (the arguments are case insensitive). Even when ``autocron.start()`` is called in the code, autocron will check the flag and will not start. The ``delay`` decorator will behave the same, returning a ``Result`` instance but the task itself will get executed in ``sync``, i.e. a blocking task will block.


Database Storage
----------------

All settings are stored in the autocron **database**. The default location for the databases is in the ``~.autocron/`` directory. When autocron starts and can not find the database, a new database is created with the default settings.

On shutting down the application, all unhandled tasks keep persistent in the autocron database and will get handled the next time the application starts. If the database file gets deleted, the contents are lost. Otherwise it is safe to delete the databases – next time when the application starts, autocron will create a fresh one.




