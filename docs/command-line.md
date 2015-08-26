# Command-line

## Configuration flags

All the command-line tools support a set of common configuration flags, defined in [config.py](https://github.com/pricingassistant/mrq/blob/master/mrq/config.py). Use --help with any of them to see the full list.

The following general flags can be passed as command-line arguments to either **mrq-worker**,**mrq-run**, or **mrq-dashboard**:

 - `--trace_greenlets`: Collect stats about each greenlet execution time and switches. Defaults to **false**.
 - `--trace_memory`: Collect stats about memory for each task. Incompatible with --greenlets > 1. Defaults to **false**.
 - `--trace_io`: Collect stats about all I/O operations. Defaults to **true**.
 - `--print_mongodb`: Print all MongoDB requests. Defaults to **false**.
 - `--trace_memory_type`: Create a .png object graph in trace_memory_output_dir with a random object of this type.
 - `--trace_memory_output_dir`: Directory where to output .pngs with object graphs. Defaults to folder **memory_traces**.
 - `--profile`: Run profiling on the whole worker. Defaults to **false**.
 - `--mongodb_jobs, --mongodb`: MongoDB URI for the jobs, scheduled_jobs & workers database. Defaults to **mongodb://127.0.0.1:27017/mrq**.
 - `--mongodb_logs` :MongoDB URI for the logs database."0" will disable remote logs, "1" will use main MongoDB. Defaults to **1**
 - `--mongodb_logs_size`: If provided, sets the log collection to capped to that amount of bytes.
 - `--no_mongodb_ensure_indexes`: If provided, skip the creation of MongoDB indexes at worker startup.
 - `--redis`: Redis URI. Defaults to **redis://127.0.0.1:6379**.
 - `--redis_prefix`: Redis key prefix. Defaults to "mrq".
 - `--redis_max_connections`: Redis max connection pool size. Defaults to **1000**.
 - `--redis_timeout`: Redis connection pool timeout to wait for an available connection. Defaults to **30**.
 - `--name`: Specify a different name.
 - `--quiet`: Don't output task logs. Defaults to **false**.
 - `--config, -c`: Path of a config file.
 - `--worker_class`: Path to a custom worker class. Defaults to **"mrq.worker.Worker"**.
 - `--version, -v`: Prints current MRQ version. Defaults to  **false**.
 - `--no_import_patch`: Skips patching __import__ to fix gevent bug #108. Defaults to **false**.
 - `--add_network_latency`: Adds random latency to the network calls, zero to N seconds. Can be a range (1-2)'). Defaults to **0**.
 - `--default_job_result_ttl`: Seconds the results are kept in MongoDB when status is success. Defaults to **604800** seconds which is 7 days.
 - `--default_job_abort_ttl`: Seconds the tasks are kept in MongoDB when status is abort. Defaults to **86400** seconds which is 1 day.
 - `--default_job_cancel_ttl`: Seconds the tasks are kept in MongoDB when status is cancel. Defaults to **86400** seconds which is 1 day.
 - `--default_job_timeout`: In seconds, delay before interrupting the job. Defaults to **3600** seconds which is 1 hour.
 - `--default_job_max_retries`: Set the status to "maxretries" after retrying that many times. Defaults to **3** seconds.
 - `--default_job_retry_delay`: Seconds before a job in retry status is requeued again. Defaults to **3** seconds.
 - `--use_large_job_ids`: Do not use compacted job IDs in Redis. For compatibility with 0.1.x only. Defaults to **false**.

## mrq-worker

`mrq-worker` starts a new worker and takes one argument list:

 - `queues`: The queues to listen on.Defaults to **default** , which will listen on all queues. 

You can pass additional configuration flags:

 - `--max_jobs`: Gevent:max number of jobs to do before quitting. Use as a temporary workaround for memory leaks in your tasks. Defaults to **0**
 - `--max_memory`: Max memory (in Mb) after which the process will be shut down. Use with `--processes [1-N]` 
                  to have supervisord automatically respawn the worker when this happens. Defaults to **1**
 - `--grenlets, --gevent, --g`: Max number of greenlets to use. Defaults to **1**.
 - `--processes, --p`: Number of processes to launch with supervisord. Defaults to **0** (no supervisord).
 - `--supervisord_template`: Path of supervisord template to use. Defaults to **supervisord_templates/default.conf**.
 - `--scheduler`: Run the scheduler. Defaults to **false**.
 - `--scheduler_interval`: Seconds between scheduler checks. Defaults to **60** seconds, only ints are acceptable.
 - `--report_interval`: Seconds between worker reports to MongoDB. Defaults to **10** seconds, floats are acceptable too.
 - `--report_file`: Filepath of a json dump of the worker status. Disabled if none.
 - `--admin_port`: Start an admin server on this port, if provided. Incompatible with --processes. Defaults to **0**
 - `--admin_ip`: IP for the admin server to listen on. Use "0.0.0.0" to allow access from outside. Defaults to **127.0.0.1**.
 - `--local_ip`: Overwrite the local IP, to be displayed in the dashboard.
 - `--max_latency`: Max seconds while worker may sleep waiting for a new job. Can be < 1 and a float value.

### Worker concurrency

The default is to run tasks one at a time. You should obviously change this behaviour to use Gevent's full capabilities with something like:

`mrq-worker --processes 3 --greenlets 10 queue-highpriority queue-default`

This will start 30 greenlets over 3 UNIX processes. Each of them will run 10 jobs at the same time.

As soon as you use the `--processes` option (even with `--processes=1`) then supervisord will be used to control the processes. It is quite useful to manage long-running instances.

### Simulating network latency

Sometimes it is helpful in local development to simulate an environment with higher network latency.

To do this we added a ```--add_network_latency=0.1``` config option that will add (in this case) a random delay between 0 and 0.1 seconds to every network call.

## mrq-dashboard

`mrq-dashboard` starts the web dashboard on the default port and takes these arguments:

 - `--dashboard_httpauth`: HTTP Auth for the Dashboard. Format is user:pass.
 - `--dashboard_queue`: Default queue for dashboard actions.
 - `--dashboard_port`: Use this port for mrq-dashboard. Defaults to port **5555**.
 - `--dashboard_ip`: Bind the dashboard to this IP. Default is **0.0.0.0**, use **127.0.0.1** to restrict access.

## mrq-run

`mrq-run` runs a one-off task. If you add the `--queue` option that will enqueue it to be later ran by a worker.
 
 - `taskpath`: Task to run.
 - `taskargs`: JSON-encoded arguments, or "key value" pairs.
 - `--queue`: Queue the task on this queue instead of running it right away.
 
Typical usage is:

```
$ mrq-run tasks.mylib.myfile.MyTask '{"param1": 1, "param2": True}'

# Shorter syntax which casts all values as strings (equivalent to '{"param1": "1", "param2": "ok"}')
$ mrq-run tasks.mylib.myfile.MyTask param1 1 param2 ok
```

