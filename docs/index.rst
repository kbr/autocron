
autocron documentation
======================

**autocron** is a simple asynchronous background task library to execute tasks in a separate process, not blocking the main application otherwise.

**autocron** does not need any dependencies beside the Python Standard-Library and is easy to install and easy to integrate into web-applications, like django, flask, pyramid, bottle, tornado or starlette. See usage for details.

**autocron** makes use of a ``SQLite`` database as message storage. This is fast enought for low- to medium-traffic sites. Which are most websites.

    The idea behind ``autocron`` is to make the integration of an asynchronous task handler as easy as possible. This is often desirable for applications that don't need massive scaling. Vertical scaling will work but for horizontal scaling autocron is currently not designed.

All configurations are preset with useful values and can get inspected and modified by the ``autocron`` command-line based :ref:`Admin Interface <admin-iterface>`.


Installation
------------

The **autocron** package is available on `PyPi <https://pypi.org/project/autocron/>`_ and installable by `pip <https://pypi.org/project/pip/>`_:  ::

    $ pip install autocron

(autocron requires Python >= 3.8)



History
-------

**autocron** is the successor of `autotask <https://github.com/kbr/autotask>`_  which was introduced in 2016 as a django-application to replace Celery on a given project for making monitoring and maintenance easier. Since then it has been used in production without any flaws (at least none has popped up in the used project). But it has had strong roots in Python 2, needed improvements for django >= 3.0 and was also bound to the django ORM for message handling. All this should be removed in an update. Also there is another (unrelated) tool named "autotask" out there in the wild. Therefore the rewrite and renaming.

---

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   source/integration
   source/application_api
   source/admin_api
   source/version_history
   source/license

