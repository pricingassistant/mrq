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

You may want to convert your logs db to a capped collection : ie. run db.runCommand({"convertToCapped": "mrq_jobs", "size": 10737418240})


Configuration
=============

Check all the [available config options](mrq/config.py)

For each of these values, configuration is loaded in this order by default:
- Command-line arguments (`mrq-worker --redis=redis://127.0.0.1:6379`)
- Environment variables prefixed by MRQ_ (`MRQ_REDIS=redis://127.0.0.1:6379 mrq-worker`)
- Python variables in a config file, by default `mrq-config.py` (`REDIS="redis://127.0.0.1:6379"` in this file)

Most of the time, you want to set all your configuration in a `mrq-config.py` file in the directory where you will launch your workers, and override some of it from the command line.

On Heroku, environment variables are very handy because they can be set like `heroku config:set MRQ_REDIS=redis://127.0.0.1:6379`


Command-line use
================

All the command-line tools support a set of common configuration flags, defined in [config.py](https://github.com/pricingassistant/mrq/blob/master/mrq/config.py). Use --help with any of them to see the full list.

 - `mrq-worker` starts a worker
 - `mrq-dashboard` starts the web dashboard on the default port
 - `mrq-run` runs a task. If you add the `--async` option that will enqueue it to be later ran by a worker

Typical usage is:
```
$ mrq-run tasks.mylib.myfile.MyTask '{"param1": 1, "param2": True}'
```


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


Raw queues
==========

With regular queues, MRQ stores the task metadata in MongoDB and the task IDs in a Redis list. This design allows a good compromise between performance and visibility.

Raw queues give you more performance and some new features in exchange for a bit less visibility. In their case, only the parameters of a task are stored in serialized form in Redis when queued, and they are inserted in MongoDB only after being dequeued by a worker.

There are 4 types of raw queues. The type of a queue is determined by a suffix in its name:

 * ```_raw``` : The simpliest raw queue, stored in a Redis LIST.
 * ```_set``` : Stored in a Redis SET. Gives you the ability to have "unique" tasks: only one (task, parameters) couple can be queued at a time.
 * ```_sorted_set``` : The most powerful MRQ queue type, stored in a Redis ZSET. Allows you to order (and re-order) the tasks to be dequeued. Like ```_set```, task parameters will be unique.
 * ```_timed_set``` : A special case of ```_sorted_set```, where tasks are sorted with a UNIX timestamp. This means you can schedule tasks to be executed at a precise time in the future.

Raw queues need a special entry in the configuration to unserialize their "raw" parameters in a regular dict of parameters. They also need to be linked to a regular queue ("default" if none) for providing visibility and retries, after they are dequeued from the raw queue.

This is an example of raw queue configuration:

```python
RAW_QUEUES = {
  "myqueue_raw": {
      "job_factory": lambda rawparam: {
          "path": "tests.tasks.general.Add",
          "params": {
              "a": int(rawparam.split(" ")[0]),
              "b": int(rawparam.split(" ")[1])
          }
      },
      "retry_queue": "high"
  }
}
```

This task adds two integers. To queue tasks, you can do from the code:

```python
from mrq.queue import send_raw_tasks

send_raw_tasks("myqueue_raw", [
  ["1 1"],
  ["42 8"]
])
```

To run them, start a worker listening to both queues:

```
$ mrq-worker high myqueue_raw
```

Queueing on timed sets is a bit different, you can pass unix timestamps directly:

```python
from mrq.queue import send_raw_tasks
import time

send_raw_tasks("myqueue_timed_set", {
  "rawparam_xxx": time.time(),
  "rawparam_yyy": time.time() + 3600  # Do this in an hour
})
```

For more examples of raw queue configuration, check https://github.com/pricingassistant/mrq/blob/master/tests/fixtures/config-raw1.py


Hunting memory leaks
====================

Memory leaks can be a big issue with gevent workers because several tasks share the same python process.

Thankfully, MRQ provides tools to track down such issues. Memory usage of each worker is graphed in the dashboard and makes it easy to see if memory leaks are happening.

When a worker has a steadily growing memory usage, here are the steps to find the leak:

 * Check which jobs are running on this worker and try to isolate which of them is leaking and on which queue
 * Start a dedicated worker with ```--trace_memory --gevent 1``` on the same queue : This will start a worker doing one job at a time with memory profiling enabled. After each job you should see a report of leaked object types.
 * Find the most unique type in the list (usually not 'list' or 'dict') and restart the worker with ```--trace_memory --gevent 1 --trace_memory_type=XXX --trace_memory_output_dir=memdbg``` (after creating the directory memdbg).
 * There you will find a graph for each task generated by [objgraph](https://mg.pov.lt/objgraph/) which is incredibly helpful to track down the leak.


Simulating network latency
==========================

Sometimes it is helpful in local development to simulate an environment with higher network latency.

To do this we added a ```--add_network_latency=0.1``` config option that will add (in this case) a random delay between 0 and 0.1 seconds to every network call.


Worker concurrency
==================

The default is to run tasks one at a time. You should obviously change this behaviour to use Gevent's full capabilities with something like:

`mrq-worker --processes 3 --gevent 10`

This will start 30 greenlets over 3 UNIX processes. Each of them will run 10 jobs at the same time.

As soon as you use the `--processes` option (even with `--processes=1`) then supervisord will be used to control the processes. It is quite useful to manage long-running instances.

On Heroku's 512M dynos, we have found that for IO-bound jobs, `--processes 4 --gevent 30` may be a good setting.


Metrics & Graphite
==================

MRQ doesn't support sending metrics to Graphite out of the box but makes it extremely easy to do so.

All you have to do is add this hook in your mrq-config file:

```

# Install this via pip
import graphiteudp

# Initialize the Graphite UDP Client
_graphite_client = graphiteudp.GraphiteUDPClient(host, port, prefix, debug=False)
_graphite_client.init()

def METRIC_HOOK(name, incr=1, **kwargs):

  # You can use this to avoid sending too many different metrics
  whitelisted_metrics = ["queues.all.", "queues.default.", "jobs."]

  if any([name.startswith(m) for m in whitelisted_metrics]):
    _graphite_client.send(name, incr)


```

If you have another monitoring system you can plug anything in this hook to connect to it!


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
