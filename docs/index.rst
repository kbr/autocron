
autocron documentation
======================

**autocron** is an asynchronous background task library to execute tasks in separate processes, not blocking the main application otherwise.

**autocron** does not need any dependencies beside the Python Standard-Library and is easy to install and to integrate to web-applications like `flask <https://flask.palletsprojects.com>`_ or `django <https://www.djangoproject.com/>`_. Because task registration is non-blocking autocron can also be used with async frameworks like `starlette <https://www.starlette.io/>`_ and `tornado <https://www.tornadoweb.org/>`_. See :ref:`Integration<integration>` for more details.

**autocron** makes use of the ``SQLite`` database as storage and handles multi-processing access to the database in a non-blocking way for the application. ``SQLite`` is fast enough for low- to medium-traffic sites – which are most websites.

    **The idea** behind ``autocron`` is to make the integration of an asynchronous background task handler for web-applications as easy as possible. ``autocron`` should be the **fire and forget** background task handler for python web-frameworks. Also there is no adminstration overhead to manage consumer or worker processes. The target audience are web-applications that don't need massive scaling, vertical scaling will work. For horizontal scaling autocron is currently not designed (but may be in the future).

All configurations are preset with default values and can get inspected and modified by the ``autocron`` command-line based :ref:`Admin Interface <admin-iterface>`.


Installation
------------

**autocron** requires Python ``>= 3.8``. The package is available on `PyPi <https://pypi.org/project/autocron/>`_ and installable by `pip <https://pypi.org/project/pip/>`_:  ::

    $ pip install autocron

A `conda <https://conda.io>`_  package is also available: ::

    $ conda install autocron


Quickstart
----------

**autocron** is designed for easy usage and easy integration to web-applications. The library provides two decorators: **cron** for recurring tasks and **delay** to delegate long running tasks to background processes. The background processes are activated calling the **start** function.

Here is an example how to use autocron with the **flask** web-framework: ::

    # application.py
    import time

    import autocron
    from flask import Flask

    app = Flask(__name__)
    autocron.start("project.db", workers=2)

    @autocron.cron("* * * * *")
    def cronjob():
        # do something from time to time ...
        print("get executed every minute")

    @autocron.delay
    def do_this_later():
        # time consuming task here ...
        time.sleep(2)
        print("do this later")

    @app.route("/later")
    def later():
        do_this_later()
        return "delayed action triggered"


The command ``$ flask --app application run`` will start the application and ``autocron.start()`` starts the background workers. When the application stops, the workers are also shut down. This is also the case if the application terminates unexpectedly – even in case of a ``kill 9``.

For the ``start()`` function the argument ``workers`` is optional and can also be set by the cli-admin-tool. For simple applications one up to four workers should be enough (as a rule of thumb don't start more workers than available cpu cores). If a worker dies, autocron detects this and starts a new one.

Beside flask **autocron** can work with other web-frameworks. See :ref:`Integration<integration>` for details.



---

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   source/integration
   source/application_api
   source/admin_api
   source/version_history
   source/license


---

    The **autocron-image** is a DiffusionBee variant of people on Mars, watching the sunset, the worker planets and having a drink (letting autocron do all the tasks :)
