
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

**autocron** provides two decorators to mark functions to get executed later or at specific times. These are ``cron`` and ``delay`` that can be used like: ::

    from autocron import cron, delay

    @cron("* 2 * * *")  # run every day at 2 am
    def cleanup_archive():
        # do some periodic cleanup here ...

    @delay
    def send_confirmation(address, message):
        # don't wait for the mailserver and delegate this
        # or any other long running task to a background process


Integration
-----------

To make the decorators work **autocron** must get started somehow. This happens in the application itself like ::

    import autocron
    autocron.start("project.db")

where "project.db" is the name of the database-file that get used as a message storage. How to do this depends on the used framework.

django
......


flask
.....



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

