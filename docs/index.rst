
autocron documentation
======================

**autocron** is an asynchronous background task library to execute tasks in separate processes, not blocking the main application otherwise.

**autocron** does not need any dependencies beside the Python Standard-Library and is easy to install and to integrate to web-applications like `django <https://www.djangoproject.com/>`_, `flask <https://flask.palletsprojects.com>`_, `pyramid <https://trypyramid.com/>`_ or `bottle <https://bottlepy.org/>`_. Because task registering is non-blocking it can also be used with async frameworks like `starlette <https://www.starlette.io/>`_ and `tornado <https://www.tornadoweb.org/>`_.

**autocron** makes use of the ``SQLite`` database as storage and handles multi-processing access to the database in a non-blocking way for the application. ``SQLite`` is fast enough for low- to medium-traffic sites. Which are most websites.

    **The idea** behind ``autocron`` is to make the integration of an asynchronous task handler for web-applications as easy as possible. Also there is no adminstration overhead to manage consumer or worker processes. The target audience are web-applications that don't need massive scaling. However, vertical scaling will work. For horizontal scaling autocron is currently not designed (but may be in the future).

All configurations are preset with default values and can get inspected and modified by the ``autocron`` command-line based :ref:`Admin Interface <admin-iterface>`.


Installation
------------

**autocron** requires Python ``>= 3.8``. The package is available on `PyPi <https://pypi.org/project/autocron/>`_ and installable by `pip <https://pypi.org/project/pip/>`_:  ::

    $ pip install autocron

A `conda <https://conda.io>`_  package is also available: ::

    $ conda install autocron


Quickstart
----------

autocron provides two decorators: ``cron`` for recurring tasks and ``delay`` to delegate long running tasks to background processes.

Here is an example how to use autocron with the **flask** web-framework: ::

    # application.py
    import time

    import autocron
    from flask import Flask

    app = Flask(__name__)
    autocron.start("project.db")

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


The command ``$ flask --app application run`` will start the application and ``autocron.start()`` starts the background workers. When the application stops, the workers are also shut down.

**What next?** For further details refer how to :ref:`integrate<integration>` autocron to different web-frameworks.


History
-------

**autocron** is the successor of a personal django-specific solution to replace celery back in 2016. Since then this predecessor has been used in production. Because of roots to Python 2 and bindings to older django-versions, autocron is not just an update or a rewrite but also got a redesign to be framework-agnostic and mostly admin free.


---

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   source/integration
   source/application_api
   source/admin_api
   source/version_history
   source/license

