## Worker

A worker is a unit of processing, that dequeues jobs and executes them.

It is started with a list of queues to listen to, in a specific order.

It can be started with concurrency options (multiple processes and / or multiple greenlets). We call this whole group a single 'worker' even though it is able to dequeue multiple jobs in parallel.

If a worker is started with concurrency options, it will poll for waiting jobs and dispatch them to its related processes/greenlets.
For example, if we decide to use the greenlets option, under the hood, the worker will be one python process that has a pool of greenlets which will be in charge of actually running tasks.


## Statuses

At any time, a worker is in one of these statuses:

* `init`: General worker initialization
* `wait`: Waiting for new jobs from Redis
* `spawn`: Got some new jobs, greenlets are being spawned
* `full`: All the worker pool is busy executing jobs
* `join`: Waiting for current jobs to finish, no new one will be accepted
* `kill`: Killing all current jobs
* `stop`: Worker is stopped, no jobs should remain