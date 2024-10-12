
autocron documentation
======================

**autocron** is an asynchronous background task library to execute tasks in separate processes. These can be delayed tasks – like sending mails – or recurring cron jobs.

**autocron** does not need any dependencies beside the Python Standard-Library and runs out of the box.

**autocron** is easy to install and to integrate to any Python web-application like `flask <https://flask.palletsprojects.com>`_ or `django <https://www.djangoproject.com/>`_. Because task-registration is non-blocking *autocron* can also be used with async frameworks like `tornado <https://www.tornadoweb.org/>`_ or `FastAPI <https://fastapi.tiangolo.com/>`_. See :ref:`Integration<integration>` for more details and other frameworks.


    **The idea** behind *autocron* is to make the integration of asynchronous background tasks as easy as possible. There is no adminstration overhead to manage consumer and worker processes or databases. As storage ``SQLite`` is used, enabled to handle parallel connections in a non-blocking way for the application. As embedded database and because *autocron* has lightweight datastructures and queries ``SQLite`` is rather fast.

    **autocron** is designed for web-sites that don't need massive scaling – which are most web-sites. Vertical scaling will work. For horizontal scaling *autocron* is currently not designed.

All configurations are preset with default values and can get inspected and modified by the ``autocron`` command-line tool. See :ref:`Admin Interface <admin-iterface>`.


Installation
------------

**autocron** requires Python ``>= 3.8``. The package is available on `PyPi <https://pypi.org/project/autocron/>`_ and installable by `pip <https://pypi.org/project/pip/>`_:  ::

    $ pip install autocron

A `conda <https://conda.io>`_  package is also available: ::

    $ conda install autocron

The source code is available at github: `<https://github.com/kbr/autocron>`_.


Quickstart
----------

**autocron** allows easy integration to web-applications. The library provides two decorators: **cron** for recurring tasks and **delay** to delegate long running tasks to background processes. The background workers are activated calling the **start** function.

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


**The command** ``$ flask --app application run`` will start the application and ``autocron.start()`` starts two background workers. When the application stops, the workers are shut down. This is also the case if the application terminates unexpectedly – even in case of a ``kill 9``. If a worker dies, autocron detects this and starts a new one.

**The argument** ``workers`` of the ``start()`` function is optional and defaults to one (1). The number of workers to start can also be set by the autocron command line `admin-tool <admin-iterface>`_. For simple applications up to four workers should be enough. Choosing the right number of workers depends on how often tasks are excecuted and the corresponding runtime. As a rule of thumb for an upper limit don't start more workers than available cpu cores.

For more details and other web-frameworks see :ref:`Integration<integration>`.

    The **autocron-image** is a `DiffusionBee <https://diffusionbee.com/>`_ variant of people on Mars, watching the sunset, the worker planets and having a drink (letting autocron do all the tasks :)



.. toctree::
   :maxdepth: 1
   :caption: Contents:
   :hidden:

   source/integration
   source/autocron_api
   source/admin_api
   source/version_history
   source/license

