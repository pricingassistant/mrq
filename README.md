# MRQ

[MRQ](http://pricingassistant.github.io/mrq) is a distributed task queue for python built on top of mongo, redis and gevent.

Full documentation is available on [readthedocs](http://mrq.readthedocs.org/en/latest/)

/!\ MRQ is not yet ready for public use. Soon!

# Why?

MRQ is an opinionated task queue. It aims to be simple and beautiful like [RQ](http://python-rq.org) while having performances close to [Celery](http://celeryproject.org)

MRQ was first developed at [Pricing Assistant](http://pricingassistant.com) and its initial feature set matches the needs of worker queues with heterogenous jobs (IO-bound & CPU-bound, lots of small tasks & a few large ones).

# Main Features

 * **Simple code:** We originally switched from Celery to RQ because Celery's code was incredibly complex and obscure ([Slides](http://www.slideshare.net/sylvinus/why-and-how-pricing-assistant-migrated-from-celery-to-rq-parispy-2)). MRQ should be as easy to understand as RQ and even easier to extend.
 * **Great [dashboard](http://mrq.readthedocs.org/en/latest/dashboard/):** Have visibility and control on everything: queued jobs, current jobs, worker status, ...
 * **Per-job logs:** Get the log output of each task separately in the dashboard
 * **Gevent worker:** IO-bound tasks can be done in parallel in the same UNIX process for maximum throughput
 * **Supervisord integration:** CPU-bound tasks can be split across several UNIX processes with a single command-line flag
 * **Job management:** You can retry, requeue, cancel jobs from the code or the dashboard.
 * **Performance:** Bulk job queueing, easy job profiling
 * **Easy [configuration](http://mrq.readthedocs.org/en/latest/configuration):** Every aspect of MRQ is configurable through command-line flags or a configuration file
 * **Job routing:** Like Celery, jobs can have default queues, timeout and ttl values.
 * **Thorough [testing](http://mrq.readthedocs.org/en/latest/tests):** Edge-cases like worker interrupts, Redis failures, ... are tested inside a Docker container.
 * **Builtin scheduler:** Schedule tasks by interval or by time of the day
 * **Greenlet tracing:** See how much time was spent in each greenlet to debug CPU-intensive jobs.
 * **Integrated memory leak debugger:** Track down jobs leaking memory and find the leaks with objgraph.

# Dashboard Screenshots

![Job view](http://i.imgur.com/xaXmrvX.png)

![Worker view](http://i.imgur.com/yYUMCbm.png)

# Get Started

 - Make sure you have installed the [dependencies](https://mrq.readthedocs.org/en/latest/dependencies/) : Redis and Mongo
 - Install MRQ with `pip install mrq`
 - Start a mongo server with `mongod &`
 - Start a redis server with `redis-server &`


Create a sample project and write a simple task :
```
  $ mkdir test-mrq && cd test-mrq
  $ touch __init__.py
  $ vim tasks.py
```


    from mrq.task import Task
    import urllib2

    class Fetch(Task):
        def run(self, params):
            f = urllib2.urlopen(params.get("url"))
            t = f.read()
            f.close()
            return len(t)

You can now run it using `mrq-run` :
```
$ mrq-run tasks.Fetch url http://www.google.com

2014-12-18 15:44:37.869029 [DEBUG] mongodb_jobs: Connecting to MongoDB at 127.0.0.1:27017/mrq...
2014-12-18 15:44:37.880115 [DEBUG] mongodb_jobs: ... connected.
2014-12-18 15:44:37.880305 [DEBUG] Starting tasks.Fetch({'url': 'http://www.google.com'})
2014-12-18 15:44:38.158572 [DEBUG] Job None success: 0.278229s total
17655
```

You can also enqueue a few tasks with
```
$ mrq-run --async --queue fetches tasks.Fetch url http://www.google.com &&
  mrq-run --async --queue fetches tasks.Fetch url http://www.yahoo.com &&
  mrq-run --async --queue fetches tasks.Fetch url http://www.wordpress.com

2014-12-18 15:49:05.688627 [DEBUG] mongodb_jobs: Connecting to MongoDB at 127.0.0.1:27017/mrq...
2014-12-18 15:49:05.705400 [DEBUG] mongodb_jobs: ... connected.
2014-12-18 15:49:05.729364 [INFO] redis: Connecting to Redis at 127.0.0.1...
5492f771520d1887bfdf4b0f
2014-12-18 15:49:05.957912 [DEBUG] mongodb_jobs: Connecting to MongoDB at 127.0.0.1:27017/mrq...
2014-12-18 15:49:05.967419 [DEBUG] mongodb_jobs: ... connected.
2014-12-18 15:49:05.983925 [INFO] redis: Connecting to Redis at 127.0.0.1...
5492f771520d1887c2d7d2db
2014-12-18 15:49:06.182351 [DEBUG] mongodb_jobs: Connecting to MongoDB at 127.0.0.1:27017/mrq...
2014-12-18 15:49:06.193314 [DEBUG] mongodb_jobs: ... connected.
2014-12-18 15:49:06.209336 [INFO] redis: Connecting to Redis at 127.0.0.1...
5492f772520d1887c5b32881
```

Now start the dasbhoard with `mrq-dashboard &` and go check your newly created queue and job on [localhost:5555](http://localhost:5555/#jobs)

Instanciate a worker with `mrq-worker` and you can follow it on the dashboard as it executes in parallel all the enqueued jobs

```
$ mrq-worker --gevent 10 fetches

2014-12-18 15:52:57.362209 [INFO] Starting Gevent pool with 10 worker greenlets (+ report, logs, adminhttp)
2014-12-18 15:52:57.388033 [INFO] redis: Connecting to Redis at 127.0.0.1...
2014-12-18 15:52:57.389488 [DEBUG] mongodb_jobs: Connecting to MongoDB at 127.0.0.1:27017/mrq...
2014-12-18 15:52:57.390996 [DEBUG] mongodb_jobs: ... connected.
2014-12-18 15:52:57.391336 [DEBUG] mongodb_logs: Connecting to MongoDB at 127.0.0.1:27017/mrq...
2014-12-18 15:52:57.392430 [DEBUG] mongodb_logs: ... connected.
2014-12-18 15:52:57.523329 [INFO] Fetching 10 jobs from ['fetches']
2014-12-18 15:52:57.537570 [DEBUG] Starting tasks.Fetch({u'url': u'http://www.google.com'})
2014-12-18 15:52:57.567311 [DEBUG] Starting tasks.Fetch({u'url': u'http://www.google.com'})
2014-12-18 15:52:57.567747 [DEBUG] Starting tasks.Fetch({u'url': u'http://www.yahoo.com'})
2014-12-18 15:52:57.568080 [DEBUG] Starting tasks.Fetch({u'url': u'http://www.wordpress.com'})
2014-12-18 15:52:57.798167 [INFO] Fetching 6 jobs from ['fetches']
2014-12-18 15:52:58.432574 [INFO] Fetching 6 jobs from ['fetches']
2014-12-18 15:52:58.670492 [DEBUG] Job 5492f771520d1887bfdf4b0f success: 1.135268s total
2014-12-18 15:52:59.344227 [DEBUG] Job 5492f74b520d1887a38dd7c8 success: 1.816439s total
2014-12-18 15:52:59.912086 [INFO] Fetching 8 jobs from ['fetches']
2014-12-18 15:53:00.685727 [DEBUG] Job 5492f772520d1887c5b32881 success: 3.149119s total
2014-12-18 15:53:01.578981 [INFO] Fetching 9 jobs from ['fetches']
2014-12-18 15:53:01.897873 [DEBUG] Job 5492f771520d1887c2d7d2db success: 4.361895s total
2014-12-18 15:53:03.415555 [INFO] Fetching 10 jobs from ['fetches']
```

# More

Full documentation is available on [readthedocs](http://mrq.readthedocs.org/en/latest/)