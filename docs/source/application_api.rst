.. _application-iterface:

Application Interface
=====================

For the application API autocron provides the decorators ``cron`` and ``delay`` and the functions ``start()``, ``stop()`` and ``get_results()``. After importing autocron the decorators and function are accessible like i.e.  ``autocron.start()``.

Functions decorated with ``delay`` will get executes later in a separate process. Functions decorated with ``cron`` will get executed periodically.

To start the autocron background workers, the function ``start()`` must get called somewere in the code. The function ``stop()`` will stop the workers. It is not necessary to call ``stop()`` because autocron stops the workers on shutdown of the main application.



start and stop
--------------

To start the autocron background workers, call ``autocron.start(<filename>)`` with a database-filename as argument. The number of workers can be set by the admin-interface or given as an argument for ``autocron.start()``.

.. autoclass:: autocron.engine.Engine
    :members: start, stop



cron
----

A function decorated with ``cron`` should get never called from the application. Instead it will get called from autocron periodically. Because of this a ``cron``-decorated function should not get arguments. The decorator can directly imported from autocron: ::

    from autocron import cron

To register a cron-function (that means autocron is aware of the decorated function) the module where the function is defined must get imported from the application.


.. automodule:: autocron.decorators
    :noindex:
    :members: cron



Example
.......

Let's consider a newsletter should get send on Monday and Wednesday at 9:30 am. This could be configured by means of a cron-string: ::

    @cron("30 9 0,2 * *")
    def send_newsletter():
        ...

and could also be configured by keyword-arguments: ::

    @cron(minutes=[30], hours=[9], dow=[0, 2])
    def send_newsletter():
        ...



delay
-----

To use the ``delay`` decorator autocron provides a shortcut for import: ::

    from autocron import delay

Functions decorated with ``delay`` will return a ``Result`` instance (see below), wrapping the result. It is safe to ignore the return value. autocron will clean up the database from time to time to delete outdated results.

.. automodule:: autocron.decorators
    :members: delay



Result
------

A ``delay``-decorated function returns a ``Result`` instance. This is a wrapper around the delayed result. The instance provides the method ``is_ready()`` to indicate whether a task has been processed. The property ``has_error`` allows to check for errors – and in case of an error the attribute ``error_message`` holds the according error-message: ::

    @delay
    def do_this_later():
        # code here ...

    task_result = do_this_later()
    ...
    if task_result.is_ready():  # note: this is a blocking call
        if task_result.has_error:
            # something went wrong
            print(task_result.error_message)
        else:
            # the result is available:
            result = task_result.result
    else:
        # try to get the result later
        ...


If autocron is inactive the decorated function will also return a ``Result`` instance with the return value of the function.

.. autoclass:: autocron.sqlite_interface.Result
    :members: has_error, result, is_ready


