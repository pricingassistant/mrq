# Intro

[MRQ](http://pricingassistant.github.io/mrq) is a distributed task queue for python built on top of mongo, redis and gevent.

Full documentation is available on [readthedocs](http://mrq.readthedocs.org/en/latest/)

/!\ MRQ is not yet ready for public use. Soon!

# Why?

MRQ is an opinionated task queue. It aims to be simple and beautiful like [RQ](http://python-rq.org) while having performances close to [Celery](http://celeryproject.org)

MRQ was first developed at [Pricing Assistant](http://pricingassistant.com) and its initial feature set matches the needs of worker queues with heterogenous jobs (IO-bound & CPU-bound, lots of small tasks & a few large ones).

# Main Features

 * **Simple code:** We originally switched from Celery to RQ because Celery's code was incredibly complex and obscure ([Slides](http://www.slideshare.net/sylvinus/why-and-how-pricing-assistant-migrated-from-celery-to-rq-parispy-2)). MRQ should be as easy to understand as RQ and even easier to extend.
 * **Great [dashboard](dashboard.md):** Have visibility and control on everything: queued jobs, current jobs, worker status, ...
 * **Per-job logs:** Get the log output of each task separately in the dashboard
 * **Gevent worker:** IO-bound tasks can be done in parallel in the same UNIX process for maximum throughput
 * **Supervisord integration:** CPU-bound tasks can be split across several UNIX processes with a single command-line flag
 * **Job management:** You can retry, requeue, cancel jobs from the code or the dashboard.
 * **Performance:** Bulk job queueing, easy job profiling
 * **Easy [configuration](configuration.md):** Every aspect of MRQ is configurable through command-line flags or a configuration file
 * **Job routing:** Like Celery, jobs can have default queues, timeout and ttl values.
 * **Thorough [testing](tests.md):** Edge-cases like worker interrupts, Redis failures, ... are tested inside a Docker container.
 * **Builtin scheduler:** Schedule tasks by interval or by time of the day
 * **Greenlet tracing:** See how much time was spent in each greenlet to debug CPU-intensive jobs.
 * **Integrated memory leak debugger:** Track down jobs leaking memory and find the leaks with objgraph.