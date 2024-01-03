
autocron documentation
======================

**autocron** is a simple asynchronous task handler to execute tasks in a separate process for not blocking the main application otherwise.

**autocron** does not need any dependencies beside the Python Standard-Library and is therefore easy to install and easy to integrate into (web-)applications.

**autocron** makes use of a dedicated ``SQLite`` database as message storage. This is fast enought for low- to medium-traffic sites. Which are most websites.

    The idea behind ``autocron`` is to make the integration of an asynchronous task handler as easy as possible. This is often desirable for applications that don't need massive scaling. Vertical scaling will work but for horizontal scaling another concept for task-queues and -workers is required.

All configurations are preset with useful values and can get inspected and modified by the ``autocron`` command-line based :ref:`admin interface <admin-iterface>`.


Installation
------------

The **autocron** package is available on `PyPi <https://pypi.org/project/autocron/>`_ and installable by `pip <https://pypi.org/project/pip/>`_:  ::

    $ pip install autocron

(autocron requires Python >= 3.8)


Usage
-----

**autocron** provides two decorators to mark functions to get executed later or at specific times. These are ``cron`` and ``delay``. They can be used like: ::

    from autocron import cron, delay

    @cron("* 2 * * *")  # run every day at 2 am
    def cleanup():
        # do some periodic cleanup here ...

    @delay
    def send_confirmation_mail(address, message):
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

Let's consider a django-application that makes use of the ``cron`` and ``delay`` decorating functions in a "view.py" module: ::

    # view.py
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
        task_result = do_this_later()
        return HttpResponse(f"Hello, world. TaskResult is waiting: {task_result.is_waiting}")

To activate autocron in a django-project, the proper way to do this is in the ``apps.py`` module of one of the django-applications. Consider the name ``djangoapp`` for one of these applications, then the content of the corresponding ``apps.py`` module may look like: ::

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

For flask autocron must get imported and started somewhere. In the following example ``autocron.start()`` is called at the end of the module: ::

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
        time.sleep(3)
        print("\ndo this later")

    @app.route("/")
    def hello_world():
        task_result = do_this_later()
        return f"TaskResult is waiting: {task_result.is_waiting}"

    autocron.start("the_flask_app.db")

It would also work if autocron gets started right at the beginning of the module like: ::

    app = Flask(__name__)
    autocron.start("the_flask_app.db")


bottle
......

For a bottle-application at least two files are recommended to use autocron. This is because bottle may start the application as main-module and there is no reliable way to get the real name of the main-module from another module imported to the main module. For this reason autocron-decorated functions should not get defined in the main-module. For example here is a bottle-application in a file named "application.py" that may get started like ``$ python application.py``: ::

    # application.py
    import autocron
    from bottle import route, run
    from utils import do_this_later

    @route('/hello')
    def hello():
        task_result = do_this_later()
        return f"Hello World! TaskResult is waiting: {task_result.is_waiting}"

    autocron.start("the_bottle_app.db")
    run(host='localhost', port=8080)

autocron gets imported and started before ``bottle.run()`` is called. The ``do_this_later()`` function is imported from "utils.py", also a cronjob-function is defined there: ::

    # utils.py
    import time
    import autocron

    @autocron.delay
    def do_this_later():
        time.sleep(2)
        print("\ndo this later")

    @autocron.cron("* * * * *")
    def cronjob():
        """do something from time to time"""
        print("action from the cron job")

This way autocron is able to import the functions later to execute them in another process. Of course bottle-applications can get started in other ways, not causing the problem to resolve the name of the main-module, however it is best to avoid a situation like this at all.


pyramid
.......

There are different ways to start pyramid for development or for production. Like bottle it is possible to start a pyramid development-server from the command line like ``$ python application.py`` and then for the same reasons the autocron decorated functions should get defined in another module that gets imported from the main-module: ::

    # application.py
    from wsgiref.simple_server import make_server
    from pyramid.config import Configurator
    from pyramid.response import Response

    import autocron
    from utils import do_this_later

    def hello_world(request):
        task_result = do_this_later()
        return Response(f"Hello World! TaskResult is waiting: {task_result.is_waiting}")

    autocron.start("the_pyramid_app.db")

    if __name__ == "__main__":
        with Configurator() as config:
            config.add_route("hello", "/")
            config.add_view(hello_world, route_name="hello")
            app = config.make_wsgi_app()
        server = make_server("0.0.0.0", 6543, app)
        server.serve_forever()

In the above example ``autocron.start()`` is not called in the ``__main__`` block to get also started if the "application.py" module gets imported itself. The "utils.py" file is the same as in the bottle-example.



async frameworks
................

    First there may be the question whether an asynchronous background task-handler like **autocron** makes sense at all in combination with async frameworks. It is the nature of these frameworks to do asynchronous tasks out of the box. However, i.e. for cron-tasks the logic must get implemented somewhere and the delayed tasks have to be handled in the framework-internal thread- or process-pools anyway, like any other blocking functions. And all these tasks must get handed around in the main-event-loop beside all other requests. autocron provides a way to delegate this to an external process. The next sections show how to do this with ``tornado`` and ``starlette``.


tornado
.......

The tornado example is similiar to the pyramid and bottle examples, the decorated functions are imported from the "utils.py" module (same code): ::

    # application.py
    import asyncio
    import tornado
    import autocron
    from utils import do_this_later

    class MainHandler(tornado.web.RequestHandler):
        def get(self):
            task_result = do_this_later()
            self.write(f"Hello, world! TaskResult is waiting: {task_result.is_waiting}")

    def make_app():
        return tornado.web.Application([
            (r"/", MainHandler),
        ])

    async def main():
        autocron.start("the_tornado_app.db")
        app = make_app()
        app.listen(8888)
        shutdown_event = asyncio.Event()
        await shutdown_event.wait()

    if __name__ == "__main__":
        asyncio.run(main())

autocron gets imported and then started from the ``main()`` function. The call of the ``delay``-decorated ``do_this_later()`` function must not get adapted to an async call (with ``async`` or `` await``), because the decorators are non-blocking (at least they run fast).

starlette
.........

starlette already comes with a buildin ``BackgroundTask`` class that can handle additional tasks after finishing the current request first. With autocron the background-task will get decoupled from the process handling the request. Also Exceptions will not have side-effects to other background-tasks and cron-tasks are simple to manage. Again it is a design-decision whether to use starlette with autocron. Here is an example how to integrate autocron in a starlette-application: ::

    # application.py
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route

    import autocron
    from utils import do_this_later

    def homepage(request):
        task_result = do_this_later()
        return PlainTextResponse(f"Hello, world! TaskResult is waiting: {task_result.is_waiting}")

    def startup():
        print("Ready to go")
        autocron.start("the_starlette_app.db")

    routes = [
        Route("/", homepage),
    ]

    app = Starlette(debug=True, routes=routes, on_startup=[startup])

The above example can get started from the command-line by ``$ uvicorn application:app``. As in the tornado example the decorated functions are imported from the "utils.py" module (same code). starlette allows to invoke a ``startup()``-function, which is the right place to call ``autocron.start()``.


History
-------

**autocron** is the successor of `autotask <https://github.com/kbr/autotask>`_  which was introduced in 2016 as a django-application to replace Celery on a given project for making monitoring and maintenance easier. Since then it has been used in production without any flaws (at least none has popped up in the used project). But it has had strong roots in Python 2, needed improvements for django >= 3.0 and was also bound to the django ORM for message handling. All this should be removed in an update. Also there is another (unrelated) tool named "autotask" out there in the wild. Therefore the rewrite and renaming.

---

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   source/decorators
   source/admin
   source/version_history
   source/license

