
Decorators
==========

**autocron** provides two decorators as external API to mark functions as cron-tasks or to get executed later. These are ``delay`` and ``cron``.



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

.. autoclass:: TaskResult
    :members: has_error, is_ready, is_waiting, result


cron
----

A function decorated with ``cron`` should get never called from the application. Instead it will get called fron autocron periodically. Because of this a ``cron``-decorated function should not get arguments. To import the decorator autocron provides a shortcut: ::

    from autocron import cron

To register a cron-function the module of the function must get imported from the application. There is no limitation of the number of cron-functions to register.


.. automodule:: autocron.decorators
    :members: cron

