
autocron documentation
======================

**autocron** is a asynchronous background task library to execute tasks in a separate process, not blocking the main application otherwise.

**autocron** does not need any dependencies beside the Python Standard-Library and is easy to install and to integrate to web-applications, like django, flask, pyramid or bottle. Because task registering is non-blocking it can also be used with async frameworks.

**autocron** makes use of the ``SQLite`` database as storage and handles multi-processing access to the database. ``SQLite`` is fast enough for low- to medium-traffic sites. Which are most websites.

    **The idea** behind ``autocron`` is to make the integration of an asynchronous task handler for web-applications as easy as possible. This is often desirable also for applications that don't need massive scaling. Vertical scaling will work. For horizontal scaling autocron is currently not designed.

All configurations are preset with useful values and can get inspected and modified by the ``autocron`` command-line based :ref:`Admin Interface <admin-iterface>`.


Installation
------------

**autocron** requires Python >= 3.8. The package is available on `PyPi <https://pypi.org/project/autocron/>`_ and installable by `pip <https://pypi.org/project/pip/>`_:  ::

    $ pip install autocron


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

**What next?** For further details refer how to :ref:`integrate<integration>` autocron with different web-frameworks.


History
-------

**autocron** is the successor of `autotask <https://github.com/kbr/autotask>`_  which was written in 2016 as a django-application to replace Celery on a given project for making monitoring and maintenance easier. Since then it has been used in production. But it has strong roots in Python 2 and is bound to older django versions. **autocron** removes all this. Also another (unrelated) tool named "autotask" is out there in the wild. Therefore the rewrite and renaming.


---

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   source/integration
   source/application_api
   source/admin_api
   source/version_history
   source/license

