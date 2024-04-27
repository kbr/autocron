Version History
===============


development
-----------

- conda installation package added
- the delay decorator can take optional arguments to specify a defined delay


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
- fix: don't register crontasks twice


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
