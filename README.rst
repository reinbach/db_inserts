Evaluate DB Inserts
===================

Runs a set number of inserts against a few different databases and times them. Very simplistic.

1. Mutliple single inserts (multiple queries)
2. Single multiple insert (single query)
3. Database specific bulk insert

Supported Databases:
--------------------

- PostgreSQL
- MySQL
- SQLite

Setup:
------

You need to create the relevant database instances for each of the databases you want to run this evaluation against.
Edit the src/evaluate.py file and comment out the databases (EVALUATE_DB) and the # of inserts (NUMBER_INSERTS) you want to ignore

Run:
----

python src/evaluate.py


TODO:
-----
- move configs into separate file
- add other databases (Redis, CouchDB)
- number of times to run evaluations becomes var, currently 1
- run calculations on results (% diffs)
- better display of results (text, graphically)
- apply better stats analysis on results (median, mean, max, min... learn stats)
- add frameworks (Flask, bottle, brubeck, django, webpy)
- add ORMs (django, sqlalchemy)

