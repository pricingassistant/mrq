MRQ
===

Mongo Redis Queue - A distributed worker task queue in Python

/!\ MRQ is not yet ready for public use. Soon!

Why?
====

MRQ is an opinionated task queue. It aims to be simple and beautiful like http://python-rq.org while having performance close to http://celeryproject.org

MRQ was first developed at http://pricingassistant.com and its initial feature set matches the needs of worker queues with heterogenous jobs (IO-bound & CPU-bound, lots of small tasks & a few large ones).

The main features of MRQ are:

 * **Simple code:** We originally switched from Celery to RQ because Celery's code was incredibly complex and obscure ([Slides](http://www.slideshare.net/sylvinus/why-and-how-pricing-assistant-migrated-from-celery-to-rq-parispy-2)). MRQ should be as easy to understand as RQ and even easier to extend.
 * **Great dashboard:** Have visibility and control on everything: queued jobs, current jobs, worker status, ...
 * **Per-job logs:** Get the log output of each task separately in the dashboard
 * **Gevent worker:** IO-bound tasks can be done in parallel in the same UNIX process for maximum throughput
 * **Supervisord integration:** CPU-bound tasks can be split across several UNIX processes with a single command-line flag
 * **Job management:** You can retry, requeue, cancel jobs from the code or the dashboard.
 * **Performance:** Bulk job queueing, easy job profiling
 * **Easy configuration:** Every aspect of MRQ is configurable through command-line flags or a configuration file
 * **Job routing:** Like Celery, jobs can have default queues, timeout and ttl values.
 * **Thorough testing:** Edge-cases like worker interrupts, Redis failures, ... are tested inside a Docker container.
 * **Builtin scheduler:** Schedule tasks by interval or by time of the day
 * **Greenlet tracing:** See how much time was spent in each greenlet to debug CPU-intensive jobs.

Performance
===========

On a MacbookPro, we see 1300 jobs/second in a single worker process with very simple jobs that store results, to measure the overhead of MRQ. However what we are really measuring there is MongoDB's write performance.

Tests
=====

Testing is done inside a Docker container for maximum repeatability. We don't use Travis-CI or friends because we need to be able to kill our process dependencies (MongoDB, Redis, ...) on demand.

```
$ make test
```

You can also open a shell inside the docker (just like you would enter in a virtualenv) with:

```
$ make docker (if it wasn't build before)
$ make ssh
```

PyPy
====

Earlier in its development MRQ was tested successfully on PyPy but we are waiting for better PyPy+gevent support to continue working on it, as performance was worse than CPython.


TODO
====

**alpha**

 * Max Retries
 * MongoDB/Redis disconnect tests in more contexts (long-running queries, ...)

**public beta**

 * Full linting
 * Code coverage
 * Public docs
 * PyPI

**whishlist**

 * task progress
 * ETAs / Lag stats for each queue
 * uniquestarted/uniquequeued via bulk sets?
 * Base cleaning/retry tasks: move
 * Current greenlet traces in dashboard
 * Move monitoring in a thread to protect against CPU-intensive tasks
 * Bulk queues
 * Tasksets
 * Full PyPy support
 * Search in dashboard

Credits
=======

Inspirations:
 * RQ
 * Celery

JS libraries used in the Dashboard:
 * http://backbonejs.org
 * http://underscorejs.org
 * http://requirejs.org
 * http://momentjs.com
 * http://jquery.com
 * http://datatables.net
 * https://github.com/Jowin/Datatables-Bootstrap3/
 * https://github.com/twbs/bootstrap

... as well as all the Python modules in requirements.txt!