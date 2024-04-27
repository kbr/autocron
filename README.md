# autocron

![](https://img.shields.io/pypi/pyversions/autocron.svg)
![](https://img.shields.io/pypi/l/autocron.svg)


**autocron** is a Python background task library with no dependencies beside the standard library. It works out of the box with webframeworks like django, pyramid, flask, bottle, tornado or starlette.

**autocron** makes it easy to delegate long running and recurring tasks to external processes. No hassle with configuration-files.

**autocron** is designed for the vast majority of webapplications that don't need massive scaling. And don't want to add unnecessary dependencies.


## Installation

For installation use pip:

```
    $ pip install autocron
```

or conda:

```
    $ conda install autocron
```

## Quickstart

autocron provides two decorators: ``cron`` that takes a string in [cron](https://en.wikipedia.org/wiki/Cron#CRON_expression)-format as argument, but accepts also keyword-arguments like *minutes* and *hours*. And ``delay`` to delegate a long running task to a background process.

Here is a simple example how to use autocron with the flask web-framework that can be run with ``$ flask --app application run``:

```
    # application.py

    import autocron
    from flask import Flask

    app = Flask(__name__)
    autocron.start("the_flask_app.db")

    @autocron.cron("* * * * *")
    def cronjob():
        # do something from time to time ...

    @autocron.delay
    def do_this_later():
        # time consuming task here ...

    @app.route("/later")
    def later():
        do_this_later()
        return "delayed action triggered"
```

After creating the flask ``app`` instance calling ``autocron.start(<databasename>)`` starts the background workers. The ``cron`` decorated ``cronjob()`` function will get executed every minute and the ``delay`` decorated ``do_this_later()`` function gets delegated to the background worker every time the application processes the ``/later`` url. Terminating the application will shut down the worker processes.

More information and examples how to use autocron with other frameworks are at the documentation.


## Documentation

The full documentation and release notes are at [https://autocron.readthedocs.org](https://autocron.readthedocs.org)


