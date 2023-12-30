
Internal Structure
==================

**autocron** is strutured into several modules.


Modules
-------


- ``decorators.py`` Decorators are executable code and for every decorated callable autocron can generate a waiting task in the autocron database. For ``cron`` decorated functions this happens at application startup. For ``delay`` this happens any time when the function gets called in the application at runtime.

- ``engine.py`` The engine is responsible to start/stop and monitor the worker processes.

- ``worker.py`` The worker fetches tasks on due from time to time and executes them in a separate process.

- ``sql_interface.py`` This is the interface to the ``SQLite`` database used as a storage for message transfer from the application to the worker.

- ``admin.py`` This is the command line interface to the autocron database to inspect the state, the configuration and to change default values.

