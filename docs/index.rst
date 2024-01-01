
autocron documentation
======================

**autocron** is a simple asynchronous task handler to execute tasks in a separate process for not blocking the main application otherwise.

**autocron** does not need any dependencies beside the Python Standard-Library and is therefore easy to install and easy to integrate into (web-)applications.

**autocron** makes use of ``SQLite`` as message storage what is fast enought for low- to medium-traffic sites. Which are most websites.

    The idea behind ``autocron`` is to make the integration of an asynchronous task handler as easy as possible. This is often desirable for applications that don't need a massive scaling.

All configurations are preset with useful values and can get inspected and modified by the ``autocron`` command-line based admin tool.


Installation
------------

The **autocron** package is available on `PyPi <https://pypi.org/project/autocron/>`_ and installable by `pip <https://pypi.org/project/pip/>`_:  ::

    $ pip install autocron

*autocron requires Python >= 3.8*


Usage
-----

**autocron** provides two decorators to mark functions to get executed later or at specific times. These are ``cron`` and ``delay``. They can be used like: ::

    from autocron import cron, delay

    @cron("* 2 * * *")  # run every day at 2 am
    def cleanup_archive():
        # do some periodic cleanup here ...

    @delay
    def send_confirmation(address, message):
        # don't wait for the mailserver and delegate this
        # or any other long running task to a background process

A ``cron`` decorated function should not return a result and also should not get called from the application. Also ``cron`` accepts keyword arguments like ``minutes`` and ``hours`` instead of a cron-formated string.

A ``delay`` decorated function returns a ``TaskResult`` instance as result, no matter whether autocron is active or not. A ``TaskResult`` instance provides attributes like ``is_ready`` which is a ``boolean`` indicating whether a result is available. In this case the function result is accessible by ``TaskResult.result``. In case the result should get ignored it is safe to ignore the returned ``TaskResult`` instance. autocon deletes outdated results from time to time from the database.

**Activate and deactivate:** for development web-frameworks provide debug-settings in one or the other way. For debugging it is often desired to not run background processes at the same time. autocron provides a global flag whether to start or not. This can be set by the autocron-admin tool. After installing autocron the admin-tool is available from the command line as the ``autocron`` command: ::

    $ autocron <database-filename> --set-autocron-lock on

If this flag is set, autocron will not start. No further changes in the code are needed. To activate autocron again set the flag to ``off`` (``true`` and ``false`` are also possible as arguments).


Integration
-----------

To make the decorators work **autocron** must get started somehow. This happens in the application itself like ::

    import autocron
    autocron.start("project.db")

where "project.db" is the name of the database-file that is used as a message storage. How to do this depends on the used framework. autocron provides also a ``stop()`` method to shut down running background processes. The ``stop()``- method gets triggered on application shutdown and it is not required to call this method explicitly.

By default the database-file is stored in the ``~.autocron/`` directory. This directory will get created if it does not exist. If the filename represents an absolute path, then the absolute path is used. The path must be valide and existing.


django
......

Let's consider a django-application that makes use of the ``cron`` and ``delay`` decorators: ::

    import time
    import autocron

    from django.http import HttpResponse

    @autocron.delay
    def do_this_later():
        """example for a blocking task."""
        time.sleep(2)
        print("\ndo this later")

    @autocron.cron("* * * * *")
    def cronjob():
        """do something every minute"""
        print("action from the cron job")

    def index(request):
        """view providing the response without delay."""
        uuid = do_this_later()
        return HttpResponse(f"Hello, world. uuid: {uuid}")

To activate autocron in a django-project the proper way to do this is in the ``apps.py`` module of one of the django-applications. Consider the name ``djangoapp`` for one of these applications, then the content of the corresponding ``apps.py`` module may look like: ::

    import autocron
    from django.apps import AppConfig

    class DjangoappConfig(AppConfig):
        default_auto_field = 'django.db.models.BigAutoField'
        name = 'djangoapp'

        def ready(self):
            autocron.start("the_django_app.db")

Keep in mind to register the django-application in the ``INSTALLED_APPS`` settings. Otherwise ``ready()`` will not get called. During startup django may call ``ready()`` multiple times. Calling ``autocron.start()`` multiple times is save because autocron knows whether it is already running or not and will not start a second time.


flask
.....

For ``flask`` autocron must get imported and started somewhere. In following example ``autocron.start()`` is called at the end of the module: ::

    import time
    import autocron
    from flask import Flask

    app = Flask(__name__)

    @autocron.cron("* * * * *")
    def cronjob():
        """do something from time to time"""
        print("action from the cron job")

    @autocron.delay
    def do_this_later():
        time.sleep(2)
        print("\ndo this later")

    @app.route("/")
    def hello_world():
        print("in hello_world")
        tr = do_this_later()
        return tr.uuid

    autocron.start("the_flask_app.db")

But it would also work if autocron gets started right at the beginning of the module like: ::

    app = Flask(__name__)
    autocron.start("the_flask_app.db")



.. note::
   **autocron** is the successor of `autotask <https://github.com/kbr/autotask>`_  which was introduced in 2016 as a django-application to replace Celery on a given project for making monitoring and maintenance easier. Since then it has been used in production without any flaws (at least none has popped up in the used project). But it has had strong roots in Python 2, needed improvements for django >= 3.0 and was also bound to the django ORM for message handling. All this should be removed in an update. Also there is another (unrelated) tool named "autotask" out there in the wild. Therefore the rewrite and renaming.




.. toctree::
   :maxdepth: 1
   :caption: Contents:

   source/decorators
   source/admin
   source/internals
   source/version_history
   source/license

