MRQ
===

Mongo Redis Queue - A distributed worker task queue in Python

Why?
====

MRQ is an opinionated task queue, heavily inspired by http://python-rq.org from @nvie.

It has most of the good stuff from RQ, plus some features originally needed for its usage at http://pricingassistant.com

These new features may be useful to others and we think most of them are best practices but we clearly don't expect MRQ to be an easy replacement option for existing RQ users.

 * MongoDB for log storage & dashboard
 * Focus on speed for bulk inserts
 * Focus on tooling and visibility on the tasks
 * gevent worker by default
 * Python 2.7+ only

Tests
=====

Testing is done inside a Docker container for maximum repeatability.

```
$ make test
```

Credits
=======

Inspirations:
 * RQ
 * Celery

Vendored:
 * https://github.com/Jowin/Datatables-Bootstrap3/
 * https://github.com/twbs/bootstrap

... as well as all the modules in requirements.txt!