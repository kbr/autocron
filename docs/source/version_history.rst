Version History
===============


development
-----------



1.2.0 - 2024-10-03
------------------

- new settings-flag ``blocking-mode`` to activate/deactivate the registration thread in the main process. Useful for application development and debugging.
- the monitor now runs as a process instead as a thread.
- the monitor checks for parent-process health to allow for a graceful shutdown of the workers if the main-application terminates unexpectedly (even in case of a kill 9).
- the engine clears the kernel process slot in case the monitor dies to prevent zombies.
- the worker will shut down in case of a missing monitor, even if no SIGTERM was received.
- fix: cli tool can delete damaged database
- fix: temporary storage added for situations on start-up where registrations happen before autocron.start() is called.


1.1.2 - 2024-09-19
------------------

- fix for django.setup()


1.1.1 - 2024-08-16
------------------

- fixes for readthedocs.


1.1.0 - 2024-08-16
------------------

- delay decorator can take optional arguments to specify a defined delay.
- configuration and semaphore-flags moved to the database.
- conda installation package added.
- nox combined with ruff.
- bugfix: create default-storage directory in case it does not exist. (#3)


1.0 - 2024-04-17
----------------

- minor documentation update
- fixed some typos
- clean up nox-sessions


1.0rc1 - 2024-04-15
-------------------

- sphinx update
- minor fixes for rtd


0.9.5 - 2024-04-15
------------------

- refactoring the Result-class
- add worker argument to start()
- fix: don't register cron-tasks twice


0.9.4 - 2024-04-03
------------------

- rewrite of the data-model
- restructure of the database access
- non-blocking function registering
- worker and engine adapted
- code simplifications
- lot of fixes


0.9.3 - 2024-03-11
------------------

- changes in the sql_interface
- improved worker
- rewrite of the scheduler module
- tests changed to pytest
- fixed: reset result_ttl
- fixed: admin creates database if not existing


0.9.2 - 2024-01-14
------------------

- fixed: timeshift-bug in scheduler


0.9.1 - 2024-01-13
------------------

- Enhanced documentation structure
- admin can set result-ttl
- minor bugfixes


0.9.0 - 2024-01-11
------------------

- first public release
