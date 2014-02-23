MRQ
===

Mongo Redis Queue - A distributed worker task queue in Python

/!\ MRQ is not yet ready for public use. Soon!

Why?
====

MRQ is an opinionated task queue. It aims to be simple and beautiful like http://python-rq.org while having performance close to http://celeryproject.org

MRQ was first developed at http://pricingassistant.com and its initial feature set matches the needs of worker queues with heterogenous jobs (IO-bound & CPU-bound, lots of small tasks & a few large ones).

The main features of MRQ are:

 * **Simple code:** We originally switched from Celery to MRQ because Celery's code was incredibly complex and obscure ( [Slides](http://www.slideshare.net/sylvinus/why-and-how-pricing-assistant-migrated-from-celery-to-rq-parispy-2) ). MRQ should be as easy to understand as RQ and even easier to extend.
 * **Great dashboard:** Have visibility and control on everything: queued jobs, current jobs, worker status, ...
 * **Per-job logs:** Get the log output of each task separately in the dashboard
 * **Gevent worker:** IO-bound tasks can be done in parallel for maximum throughput
 * **Supervisord integration:** CPU-bound tasks can be split across several UNIX processes with a single command-line flag
 * **Job management:** You can retry, requeue, cancel jobs from the code or the dashboard.
 * **Performance:** Bulk job queueing, easy job profiling
 * **Easy configuration:** Every aspect of MRQ is configurable through command-line flags or a configuration file
 * **Job routing:** Like Celery, jobs can have default queues, timeout and ttl values.
 * **Thorough testing:** Edge-cases like worker interrupts, Redis failures, ... are tested inside a Docker container.

Performance
===========

On a MacbookPro, we see 1300 jobs/second with very simple jobs that store results, to measure the overhead of MRQ. However what we are really measuring there is MongoDB's write performance.

Tests
=====

Testing is done inside a Docker container for maximum repeatability. We don't use Travis-CI or friends because we need to be able to kill our process dependencies (MongoDB, Redis, ...) on demand.

```
$ make test
```

TODO
====

**alpha**

 * Max Retries
 * MongoDB/Redis interrupt tests
 * Scheduler test
 * Scheduler dailytime
 * More worker info in dashboard
 * Base cleaning/retry tasks: interrupted, move

**beta**

 * Scheduled statuses in dashboard
 * Full linting
 * Code coverage
 * Public docs
 * PyPI
 * Move monitoring in a thread?
 * Bulk queues
 * Tasksets
 * Search in dashboard


Credits
=======

Inspirations:
 * RQ
 * Celery

Vendored:
 * https://github.com/Jowin/Datatables-Bootstrap3/
 * https://github.com/twbs/bootstrap

... as well as all the modules in requirements.txt!