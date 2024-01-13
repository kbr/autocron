.. _application-iterface:

Application Interface
=====================

For the application API autocron provides the decorators ``cron`` and ``delay`` and the functions ``start()``, ``stop()`` and ``get_results()``. After importing autocron the decorators and function are accessible like i.e.  ``autocron.start()``.

Functions decorated with ``delay`` will get executes later in a separate process. Functions decorated with ``cron`` will get executed periodically.

To start the autocron background workers, the function ``start()`` must get called somewere in the code. The function ``stop()`` will stop the workers. It is not necessary to call ``stop()`` because autocron stops the workers on shutdown of the main application.


cron
----

A function decorated with ``cron`` should get never called from the application. Instead it will get called from autocron periodically. Because of this a ``cron``-decorated function should not get arguments. To import the decorator autocron provides a shortcut: ::

    from autocron import cron

To register a cron-function (that means autocron is aware of the decorated function) the module where the function is defined must get imported from the application.


.. automodule:: autocron.decorators
    :members: cron


Example
.......

Let's consider a newsletter should get send on Monday and Wednesday at 9:30 am. This could be configured by means of a cron-string: ::

    @cron("30 9 0,2 * *")
    def send_newsletter():
        ...

This could also be configured by keyword-arguments: ::

    @cron(minutes=[30], hours=[9], dow=[0, 2])
    def send_newsletter():
        ...


delay
-----

To use the ``delay`` decorator autocron provides a shortcut for import: ::

    from autocron import delay

Functions decorated with ``delay`` will return ``TaskResult`` instances (see below), wrapping the result. In case that the result can be ignored (may be the function returns no result) it is safe to ignore the return value. autocron will clean up the database from time to time to delete outdated results.

.. automodule:: autocron.decorators
    :members: delay



TaskResult
..........

A ``delay``-decorated function returns a ``TaskResult`` instance. This is a wrapper around the delayed result. The instance provide attributes like ``is_ready`` to indicate whether a result is available: ::

    @delay
    def do_this_later():
        # code here ...

    task_result = do_this_later()
    ...
    if task_result.is_ready:
        result = task_result.result
    else:
        # try to get the result later

If autocron is inactive the decorated function will not return a ``TaskResult`` instance but the original return value of the function.

.. autoclass:: autocron.sql_interface.TaskResult
    :members: has_error, is_ready, is_waiting, result


start and stop
--------------

To start the autocron background workers, call ``autocron.start(<filename>)`` with a database-filename as argument. Calling ``stop()`` is not necessary. If the application terminates, autocron stops the workers. The number of workers can get set by the admin-interface.

.. autoclass:: autocron.engine.Engine
    :members: start, stop


accessing results
-----------------

Calling a ``delay``-decorated function will return a ``TaskResult`` instance. This instance allows to access the delayed result of the function call. A call to ``autocron.get_results()`` returns a list of all available ``TaskResult`` instances.

.. autoclass:: autocron.sql_interface.SQLiteInterface
    :members: get_results

Results are deleted from the database after a timespan given by ``result_ttl``. This value defaults to 1800 seconds (30 minutes) and can get set by the admin-interface. Do not missuse the autocron database as a long-term storage for results. Instead use another dedicated database.
