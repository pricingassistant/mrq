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
 * **Integrated memory leak debugger:** Track down jobs leaking memory and find the leaks with objgraph.



Dashboard
=========

A strong focus was put on the tools and particularly the dashboard. After all it is what you will work with most of the time!

![Job view](http://i.imgur.com/xaXmrvX.png)

![Worker view](http://i.imgur.com/yYUMCbm.png)

There are too much features on the dashboard to list, but the goal is to have complete visibility and control over what your workers are doing!


Design
======

A talk+slides about MRQ's design is upcoming.

A couple things to know:
- We use Redis as a main queue for task IDs
- We store metadata on the tasks in MongoDB so they can be browsable and managed more easily.



Performance
===========

On a MacbookPro, we see 1300 jobs/second in a single worker process with very simple jobs that store results, to measure the overhead of MRQ. However what we are really measuring there is MongoDB's write performance.


Install & dependencies
======================

MRQ can be installed via PIP:

```pip install mrq```

MRQ has only been tested with Python 2.7+. External service dependencies are MongoDB >= 2.4 and Redis >= 2.6 (we use LUA scripting to boost performance and provide extra safety).

You will need [Docker](http://docker.io) to run our unit tests. Our [Dockerfile](https://github.com/pricingassistant/mrq/blob/master/Dockerfile) is actually a good way to see a complete list of dependencies, including dev tools like graphviz for memleak images.


Use in your application
=======================

- You can use `mrq-worker` to start a worker and `mrq-dashboard` to start the web dashboard on the default port.
- To run a task you can use `mrq-run`. If you add the `--async` option that will enqueue it to be later ran by a worker
- you may want to convert your logs db to a capped collection : ie. run db.runCommand({"convertToCapped": "mrq_jobs", "size": 10737418240})


Tests
=====

Testing is done inside a Docker container for maximum repeatability.
We don't use Travis-CI or friends because we need to be able to kill our process dependencies (MongoDB, Redis, ...) on demand.

Therefore you need to ([install docker](https://www.docker.io/gettingstarted/#h_installation)) to run the tests.
If you're not on an os that supports natively docker, don't forget to start up your VM and ssh into it.

```
$ make test
```

You can also open a shell inside the docker (just like you would enter in a virtualenv) with:

```
$ make docker (if it wasn't build before)
$ make ssh
```


Configuration
=============

Check all the [available config options](mrq/config.py)

For each of these values, configuration is loaded in this order by default:
- Command-line arguments (`mrq-worker --redis=redis://127.0.0.1:6379`)
- Environment variables prefixed by MRQ_ (`MRQ_REDIS=redis://127.0.0.1:6379 mrq-worker`)
- Python variables in a config file, by default `mrq-config.py` (`REDIS="redis://127.0.0.1:6379"` in this file)

Most of the time, you want to set all your configuration in a `mrq-config.py` file in the directory where you will launch your workers, and override some of it from the command line.

On Heroku, environment variables are very handy because they can be set like `heroku config:set MRQ_REDIS=redis://127.0.0.1:6379`


Job maintenance
===============

MRQ can provide strong guarantees that no job will be lost in the middle of a worker restart, database disconnect, etc...

To do that, you should add these recurring scheduled jobs to your mrq-config.py:

```
SCHEDULER_TASKS = [

  # This will requeue jobs marked as interrupted, for instance when a worker received SIGTERM
  {
    "path": "mrq.basetasks.cleaning.RequeueInterruptedJobs",
    "params": {},
    "interval": 5 * 60
  },

  # This will requeue jobs marked as started for a long time (more than their own timeout)
  # They can exist if a worker was killed with SIGKILL and not given any time to mark
  # its current jobs as interrupted.
  {
    "path": "mrq.basetasks.cleaning.RequeueStartedJobs",
    "params": {},
    "interval": 3600
  },

  # This will requeue jobs 'lost' between redis.blpop() and mongo.update(status=started).
  # This can happen only when the worker is killed brutally in the middle of dequeue_jobs()
  {
    "path": "mrq.basetasks.cleaning.RequeueLostJobs",
    "params": {},
    "interval": 24 * 3600
  }
]
```

Obviously this implies that all your jobs should be *idempotent*, meaning that they could be done multiple times, maybe partially, without breaking your app. This is a very good design to enforce for your whole task queue, though you can still manage locks yourself in your code that make sure a block of code will only run once.


Hunting memory leaks
====================

Memory leaks can be a big issue with gevent workers because several tasks share the same python process.

Thankfully, MRQ provides tools to track down such issues. Memory usage of each worker is graphed in the dashboard and makes it easy to see if memory leaks are happening.

When a worker has a steadily growing memory usage, here are the steps to find the leak:

 * Check which jobs are running on this worker and try to isolate which of them is leaking and on which queue
 * Start a dedicated worker with ```--trace_memory --gevent 1``` on the same queue : This will start a worker doing one job at a time with memory profiling enabled. After each job you should see a report of leaked object types.
 * Find the most unique type in the list (usually not 'list' or 'dict') and restart the worker with ```--trace_memory --gevent 1 --trace_memory_type=XXX --trace_memory_output_dir=memdbg``` (after creating the directory memdbg).
 * There you will find a graph for each task generated by [objgraph](https://mg.pov.lt/objgraph/) which is incredibly helpful to track down the leak.


Worker concurrency
==================

The default is to run tasks one at a time. You should obviously change this behaviour to use Gevent's full capabilities with something like:

`mrq-worker --processes 3 --gevent 10`

This will start 30 greenlets over 3 UNIX processes. Each of them will run 10 jobs at the same time.

As soon as you use the `--processes` option (even with `--processes=1`) then supervisord will be used to control the processes. It is quite useful to manage long-running instances.

On Heroku's 512M dynos, we have found that for IO-bound jobs, `--processes 4 --gevent 30` may be a good setting.


PyPy
====

Earlier in its development MRQ was tested successfully on PyPy but we are waiting for better PyPy+gevent support to continue working on it, as performance was worse than CPython.


Useful third-party utils
========================

* http://superlance.readthedocs.org/en/latest/


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
