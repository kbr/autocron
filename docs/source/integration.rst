.. _integration:

Integration
===========

**autocron** is designed for easy integration to web-applications. The public interface are the two decorators ``cron`` and ``delay`` and the ``start()`` funtion.

Just apply the decorators and call ``start()`` and you are ready to go.

Also autocron provides the settings ``autocron-lock`` and ``blocking-mode`` that can be useful for development and debugging (see below).


Decorators
----------

**autocron** provides two decorators to mark functions to get executed later or at specific times. These are ``cron`` and ``delay``. They can be used like: ::

    from autocron import cron, delay

    @cron("* 2 * * *")  # run every day at 2 am
    def cleanup():
        # do some periodic cleanup here ...

    @delay
    def send_confirmation_mail(address, message):
        # example of  a long running task:
        # don't wait for the mailserver and delegate this
        # to a background process by applying the delay-decorator

- A ``cron`` decorated function should not return a result and also should not get called from the application. However the module with a cron-decorated function must get imported from the application. Beside a cron-formated string ``cron`` also accepts keyword arguments like ``minutes`` and ``hours``

- A ``delay`` decorated function returns a ``Result`` instance as result, regardless whether autocron is active or inactive.

- A ``Result`` instance provides attributes like ``is_waiting`` which is a ``boolean`` indicating whether a result is available. In this case the function result is accessible by ``Result.function_result``. In case the result should get ignored it is safe to ignore the returned ``Result`` instance. autocon deletes outdated results from time to time from the database.

More details about the decorators and ``Result`` are in the chapter :ref:`autocron api<autocron-api>`.


Settings
--------

**Activate and deactivate:** autocron provides a global flag whether to start or not. This can be set by the autocron-admin tool. After installing autocron, the admin-tool is available from the command line: ::

    $ autocron <database-filename> --set-autocron-lock=on

If this flag is set, autocron will not start. No further changes in the code are needed. To activate autocron again, set the flag to ``off`` (``true`` and ``false`` are also possible arguments).

**blocking and non-blocking:** autocron starts a thread in the application-process to register tasks, so registering is non-blocking. To not start this thread set: ::

    $ autocron <database-filename> --set-blocking-mode=on

Default setting is ``off``. In blocking mode tasks are still executed in background processes, just the registration is a blocking step because of a database access in the main thread.

Both settings are useful for development and debugging.

More about settings: :ref:`Admin interface<admin-iterface>`.



Application-Integration
-----------------------

To make the decorators work, **autocron** has to start. This happens in the application like ::

    import autocron
    autocron.start("project.db")

with "project.db" as the name of the database-file. If the file does not exist it will get created. More details about the ``start()`` function are in the chapter :ref:`autocron api<autocron-api>`.

Where to integrate the ``start()`` function depends on the used framework:



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
        return HttpResponse(f"Hello, TaskResult uuid: {task_result.uuid}")

To activate autocron in a django-project, the proper way to do this is in the ``apps.py`` module of one of the django-applications. Consider the name ``djangoapp`` for one of these applications, then the content of the corresponding ``apps.py`` module may look like: ::

    import autocron
    from django.apps import AppConfig

    class DjangoappConfig(AppConfig):
        default_auto_field = 'django.db.models.BigAutoField'
        name = 'djangoapp'

        def ready(self):
            autocron.start("the_django_app.db")

Don't forget to register the django-application in the ``INSTALLED_APPS`` settings. Otherwise ``ready()`` will not get called. During startup django may call ``ready()`` multiple times. Calling ``autocron.start()`` multiple times is save because autocron knows whether it is already running or not.

    **Note:** the django-reloader is known for not working well with multi-threading applications. For compatibility set ``--set-blocking-mode=on`` to use autocron in blocking mode.


flask
.....

Using flask ``autocron.start()`` is called after creating the flask-app: ::

    # application.py
    import time
    import autocron
    from flask import Flask

    app = Flask(__name__)
    autocron.start("the_flask_app.db")

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
        return f"Hello, TaskResult uuid: {task_result.uuid}"

Now start flask from the command line ``$ flask --app application run`` and the application runs with background processes.


bottle
......

For a bottle-application at least two files are recommended to use autocron. This is because the bottle application may get started from the command line as the Python main-module. Unfortunately there is no reliable way to get the real name of the main-module at runtime. For this reason autocron-decorated functions should not be defined in the main-module. For example here ist a "utils.py" file with two decorated function: ::

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


The entry-point of the bottle-application is in a file named "application.py" that may get started like ``$ python application.py``: ::

    # application.py
    import autocron
    from bottle import route, run
    from utils import do_this_later

    @route('/hello')
    def hello():
        result = do_this_later()
        return f"result.uuid: {result.uuid}"

    autocron.start("the_bottle_app.db")
    run(host='localhost', port=8080)

autocron gets imported and started before ``bottle.run()`` is called, because run() will not return. The ``do_this_later()`` function is imported from "utils.py". Also the cronjob-function is imported and will get executed every minute.

(bottle-applications can also get started in other ways, not causing the problem to resolve the name of the main-module – however it is a good idea to avoid a situation like this.)


pyramid
.......

For development, a pyramid application can get started from the command-line via ``$ python application.py``, like a bottle application. For the same reason the autocron decorated functions should be defined in separate modules: ::

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


The module "utils.py" is used by the main-application: ::

    # application.py
    from wsgiref.simple_server import make_server
    from pyramid.config import Configurator
    from pyramid.response import Response

    import autocron
    from utils import do_this_later

    def hello_world(request):
        task_result = do_this_later()
        return Response(f"Hello, TaskResult uuid: {task_result.uuid}")

    autocron.start("the_pyramid_app.db")

    if __name__ == "__main__":
        with Configurator() as config:
            config.add_route("hello", "/")
            config.add_view(hello_world, route_name="hello")
            app = config.make_wsgi_app()
        server = make_server("0.0.0.0", 6543, app)
        server.serve_forever()

In the above example ``autocron.start()`` is not called in the ``__main__`` block, so it will also get called if the "application.py" module gets imported itself, i.e. after deployment for production. As in the bottle-example the cronjob will get called every minute.


async frameworks
................

    First there may be the question whether an asynchronous background task-handler like **autocron** makes sense in combination with async frameworks. It is the nature of these frameworks to do asynchronous tasks out of the box. However the way they do this may fit or not fit your needs or the way you like to handle it. Registering tasks in autocron is **non-blocking** and therefore also suitable for async frameworks.


tornado
.......

The tornado example is similiar to the pyramid and bottle examples, defining decorated functions in a separate module: ::

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


The module "utils.py" is used by the main-application: ::

    # application.py
    import asyncio
    import tornado
    import autocron
    from utils import do_this_later

    class MainHandler(tornado.web.RequestHandler):
        def get(self):
            task_result = do_this_later()
            self.write(f"Hello, TaskResult uuid: {task_result.uuid}")

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

autocron gets imported and then started from the ``main()`` function. The call of the ``delay``-decorated ``do_this_later()`` function must not get adapted to an async call (with ``async`` or `` await``), because the decorated functions are non-blocking. Also the cronjob runs every minute.


starlette
.........

starlette already comes with a buildin ``BackgroundTask`` class that can handle additional tasks after finishing the current request first. With autocron,  background-task can get decoupled from the process handling the request and it is easy to include cron-jobs. Again the decorated functions are defined in a separate module: ::

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


and imported by the main application: ::

    # application.py
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route

    import autocron
    from utils import do_this_later

    def homepage(request):
        task_result = do_this_later()
        return PlainTextResponse(f"Hello, TaskResult uuid: {task_result.uuid}")

    def startup():
        print("Ready to go")
        autocron.start("the_starlette_app.db")

    routes = [
        Route("/", homepage),
    ]

    app = Starlette(debug=True, routes=routes, on_startup=[startup])


starlette allows to invoke a ``startup()``-function, which is the right place to call ``autocron.start()``.

The above example can get started from the command-line by ``$ uvicorn application:app``. The cronjob function will get executed every minute.


FastAPI
.......

FastAPI is based on starlette and has the same backgroundtask-mechanism. But integration of autocron works a bit different as FastAPI uses a contextmanager to call functions at startup and shutdown.

The decorated functions are defined in a separate module: ::

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

and imported by the main application: ::

    import autocron

    from contextlib import asynccontextmanager
    from fastapi import FastAPI
    from utils import do_this_later

    @asynccontextmanager
    async def lifespan(app):
        autocron.start("the_fastapi_app.db", workers=4)
        try:
            yield
        finally:
            autocron.stop()  # not really needed

    app = FastAPI(lifespan=lifespan)

    @app.get("/")
    def read_root():
        do_this_later()
        return {"Hello": "World"}


The ``autocron.start()`` function is called on startup by the ``lifespan`` function. The contextmanager allows to call ``autocron.stop()`` explicitly. This is not really neccessary as autocron detects when the application terminates.

To start the FastAPI application call ``fastapi dev main.py`` or ``fastapi run main.py`` at the command line.



other frameworks
................

The above examples can get adapted to other frameworks by following two rules:

- Don't apply the ``cron`` and ``delay`` decorators to functions in a module with the internal name ``__main__`` at runtime.

- the function ``start()`` must get called somewhere before the application enters the main-event loop.

