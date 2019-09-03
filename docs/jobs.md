# Jobs & Tasks

A **Task** is a Python class wrapping a unit of processing. A task may invoke other tasks, synchronously or asynchronously (by queuing them as proper jobs).

A **Job** is an instance of the execution of a Task. It must link to a specific Task via it's path and it must have parameters. It must be queued in a Queue. A Worker then dequeues and executes it. The worker updates it with metadata about it's execution : status, traceback ...

## Job statuses

MRQ defines a list of statuses for jobs. A job can only be in one of them at a time.

When everything goes fine, a job will go through 4 statuses:

* ```delayed```: The Job has been created and it is delayed to be queued later.
* ```queued```: The Job has been created and it is waiting to be dequeued by a Worker.
* ```started```: A Worker has dequeued the job and started executing it.
* ```success```: The job was successfully ran.

However, to be reliable a task queue needs to prepare for everything that can go wrong. These statuses will help you manage those cases:

* ```failed```: A Python Exception was raised during the execution of the job. It can be an Exception you raised yourself or an error in a module you are using. MRQ's Dashboard features a view where you can have a look at the tracebacks to debug these exceptions.
* ```cancel```: The job was cancelled. This will happen mainly when you cancel jobs from the Dashboard before they run. Be careful, cancelling jobs when they are `started` won't interrupt the currently running job.
* ```abort```: The job was aborted. This happens while the job is running and `abort_current_job()` is called. Often this will be the result of an unrecoverable error that you don't want to retry but still want logged in the Dashboard for some time (see `result_ttl`)
* ```interrupt```: While running this job, the worker was interrupted and had the time to save this status. This happens when the worker process receives the UNIX signal SIGTERM or two SIGINTs (which can happen by sending Ctrl-C two times). This status won't be set if the process is interrupted with a SIGKILL or any other abrupt means like a power off, and the task will stay in `started` state until requeued or cancelled by a maintenance job.
* ```timeout```: The job took too long to finish and was interrupted by the worker. Timeouts can be set globally or for each task.
* ```retry```: The method `task.retry()` was called to interrupt the job but mark it for being retried later. This may be useful when calling unreliable 3rd-party services.
* ```maxretries```: The task was retried too many times. Max retries default to 3 and can be configured globally or per task. At this point it should be up to you to cancel them or requeue them again.

Jobs in status `success` will be cleaned from MongoDB after a delay of `result_ttl` seconds (see [Task configuration](configuration.md))

## Task API

Each of your tasks should subclass the `mrq.task.Task` class, which provides the following simple API:

`Task.run(self, params)`

The main entry point for all tasks. `params` is always a dict. The return value of this function will be stored in MongoDB.

`Task.is_main_task`

A boolean indicating whether the task is the main task of this job. If False, the task is a sub-task. This shouldn't make a difference for most apps.


## Job API

In `mrq.job` you will find methods to create jobs and enqueue them:

* `queue_job(main_task_path, params, queue=None)`

Queues a job. If `queue` is not provided, the default queue for that Task as defined in the configuration will be used. If there is none, the queue `default` will be used. Returns the ID of the job.

* `queue_jobs(main_task_path, params_list, queue=None, batch_size=1000)`

Queues multiple jobs at once. Returns a list of IDs of the jobs.

* `queue_job(main_task_path, params, delay=120, queue=None)`

Create a job with `delayed` status, the job will be queued after of at least `delay` seconds. 
Remember to add the base delayed job as explained in [Jobs maintenance](jobs-maintenance.md) to have `delayed` jobs actually queued.

* `queue_raw_jobs(queue, params_list, batch_size=1000)`

Queues multiple jobs at once on a [raw queue](queues.md#raw-queues). The queued jobs have no IDs on a raw queue so this function has no return.

* `get_job_result(job_id)`

Returns a `dict` with `result` (can be any type) and `status`.

## Context API

The Context API provides the method used to get and interact with the current greenlet context. These methods can be imported from `mrq.context`:

* `get_current_job()`

Returns the Job instance currently being executed. If None, you are outside of a Job context. This can only happen when calling code from your tasks outside of `mrq-run` or `mrq-worker`.

* `retry_current_job(delay=None, max_retries=None, queue=None)`

Interrupts the current code block and marks the job as needing to be retried in the future. Behind the scenes, it will raise a `RetryInterrupt` exception, interrupting the current code block.

The `delay` parameter defaults to the value of `retry_delay` in the task configuration, in seconds.

The `max_retries` parameter defaults to the value of `max_retries` in the task configuration. If the task has already been retried more than this, its status will be changed to `maxretries`.

If the `queue` parameter is supplied, the job will be enventually requeued on that queue. If not, it will stay on its original queue.

* `abort_current_job()`

Stops the execution of the current job (by raising an `AbortInterrupt`) and mark it with the status `abort`. It will stay visible in the Dashboard for `result_ttl` seconds.

* `set_current_job_progress(ratio)`

Shorthand for get_current_job().set_progress(). Ratio is a float between 0 and 1.

* `get_current_worker()`

Returns the current Worker instance.

* `get_current_config()`

Return the current Config dict.

* `subpool_map(pool_size, func, iterable)`

For each dict of parameters in `iterable`, execute `func` in parallel inside a pool of `pool_size` greenlets. Each of these greenlets will be able to call `get_current_job()` and get correct results. Return the results of these greenlets as a list.

* `log`

A regular Python logging object that should be used in your task code. It will make task logs available in the Dashboard.

* `connections`

A lazy-loaded object containing the worker's connections to MongoDB and Redis. You can implement your own connection factories to instanciate other services lazily.

* `run_task(path, params)`

Executes the task located at `path` synchronously and returns its result. This doesn't create a new Job context. Use this to call sub-tasks from the code of your main task.


## Helpers API

Helpers are util functions which use the current context or the configuration:

* `metric(name, incr=1, **kwargs)`

Can be used to send metrics to a 3rd-party service like Graphite.

* `ratelimit(key, limit, per=1, redis=None)`

Returns an integer with the number of available actions for the current period, in seconds. If zero, rate was already reached.
