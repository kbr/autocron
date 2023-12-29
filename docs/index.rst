
autocron documentation
======================

**autocron** is a simple asynchronous task handler to execute tasks in a separate process for not blocking the main application otherwise. Also it provides a scheduler to execute defined tasks at given times. All it takes is the useage of two provided decorators ``cron`` and ``delay`` like: ::

    from autocron import cron, delay

    @cron("* 2 * * *")
    def cleanup_archive():
        # do some periodic cleanup here ...

    @delay
    def send_confirmation(address, message):
        # don't wait for the mailserver,
        # delegate this to a background process
        code_goes_here()

**autocron** does not need any dependencies beside the Python Standard-Library and is therefore easy to install and easy to integrate into (web-)applications.

**autocron** makes use of ``SQLite`` as message storage what is fast enough for low- to medium-traffic sites - which are most websites.

The idea behind ``autocron`` is to make the integration of an asynchronous task handler as easy as possible. After installing just call ::

    import autocron
    autocron.start("project.db")

somewhere in the program to start the background worker. See Integration for details.

.. note::
   **autocron** is the successor of `autotask <https://github.com/kbr/autotask>`_  which was introduced in 2016 as a django-application to replace Celery on a given project for making monitoring and maintenance easier. Since then it has been used in production without any flaws (at least none have popped up in the used project). But it has had strong roots in Python 2, needed improvements for django >= 3.0 and was also bound to the django ORM for message handling. All this should be removed in an update. Also there is another (unrelated) tool named "autotask" out there in the wild. Therefore the rewrite and renaming.



.. toctree::
   :maxdepth: 1
   :caption: Contents:

   source/installation
   source/integration
   source/decorators
   source/admin
   source/internals
   source/version_history
   source/license

