This tutorial will guide you through the configuration of a MRQ queue for maximum performance.

Code is available in the `examples/queue_performance` folder. To be able to run the commands below, you should enter the container first:

```
make shell
make stack
cd examples/queue_performance
```



## Regular queue




### Default setup

Let's start with a simple task that squares integers, from the `tasks.py` file:

```
class Square(Task):
    def run(self, params):
        return int(params["n"]) ** 2
```

You can enqueue it 200 times on a regular, MongoDB-backed queue named `square` with this code:

```
from mrq.job import queue_jobs
queue_jobs("tasks.Square", [{"n": 42} for _ in range(200)], queue="square")
```

For convenience, we will use the `enqueue.py` file to do this. Here is the command to enqueue the jobs and launch a worker to dequeue them:

```
./enqueue.py square 200 && mrq-worker square
```

You should see the output of the worker, with a line like this one at the end (performance numbers from a 2015 MacBook Pro):

```
[INFO] Worker spent 2.398 seconds performing 200 jobs (83.403 jobs/second)
```

As we have `DEQUEUE_STRATEGY = "burst"` in the `mrq-config.py` file, the worker exits as soon as there are no jobs left on the queue, which is more convenient for this tutorial.

80 jobs per second is rather slow. The main bottleneck is that by default, `mrq-worker` uses a single process and a single greenlet. With this setup, jobs are executed sequentially and between each, the worker must fetch the next one from MongoDB. As a consequence, most of the time of the worker is spent on blocking I/O to MongoDB: not good!




### Multi-greenlet worker

Fortunately, MRQ uses [gevent](http://gevent.org) and allows us to start many greenlets at once in the same worker. Let's try with 5 greenlets:

```
./enqueue.py square 200 && mrq-worker square --greenlets 5
...
[INFO] Worker spent 0.652 seconds performing 200 jobs (306.554 jobs/second)
```

We got an almost linear increase in performance! What if we tried 50 greenlets?

```
./enqueue.py square 200 && mrq-worker square --greenlets 50
...
[INFO] Worker spent 0.382 seconds performing 200 jobs (523.174 jobs/second)
```

A nice increase again, but definitely not linear anymore. Depending on your workload, the performance gains will stop at some point either because you hit a CPU bottleneck on the worker, or the concurrency limit of your MongoDB server.

If MongoDB is the limiting factor, you have 2 choices to go further:

 - Scale your MongoDB instance ([many options](https://docs.mongodb.com/manual/administration/analyzing-mongodb-performance/) are available, including [sharding](https://docs.mongodb.com/manual/sharding/))
 - Switch to a Redis-backed queue (also called a *raw queue* in MRQ).



## Raw queue




### Default setup

A raw queue must be configured in `mrq-config.py` with its job factory function, which will transform a "raw" parameter string into a complete job definition:

```
RAW_QUEUES = {
    "square_raw": {
        "job_factory": lambda rawparam: {
            "path": "tasks.Square",
            "params": {
                "n": rawparam
            }
        }
    }
}
```

The only thing that will be queued in redis will be the raw parameter. This has the benefit of using much less storage than MongoDB-backed queues, but also of being faster to dequeue:

```
./enqueue.py square_raw 2000 && mrq-worker square_raw --greenlets 30
...
[INFO] Worker spent 1.804 seconds performing 2000 jobs (1108.419 jobs/second)
```

Much better! If you use `top` while launching these commands (you can open a second shell in the same container with the `make reshell` command from the host), you will see that the python worker process is now maxing-out a CPU.




### Multi-process worker

As you know, a single Python process can only use a single CPU. Let's try to use all the cores you have at your disposal to get better performance!

mrq-worker can start multiple processes with the ```--processes``` flag. In this case it will use `supervisord` to manage the processes. If you use this option you will have to manually terminate the worker with a `ctrl-C` keystroke once it is finished:

```
./enqueue.py square_raw 20000 && mrq-worker square_raw --greenlets 30 --processes 5
...
[INFO] Worker spent 6.697 seconds performing 4239 jobs (632.986 jobs/second)
...
[INFO] Worker spent 6.505 seconds performing 4307 jobs (662.075 jobs/second)
...
```

Each of the 5 worker processes handled its share of the jobs. The performance numbers aren't aggregated but you can see that the global throughput is now more than 3000 jobs per second.

`top` reveals that the bottleneck is once again MongoDB. We are using a Redis-backed queue so jobs are not queued in MongoDB anymore but by default they are still inserted there once they are started. This is done to be able to see them in MRQ's dashboard as well as to store their results once they reach the `success` state.




### Redis-only queue

If you don't need visibility on started jobs or on their results, you can actually bypass MongoDB altogether with this configuration:

```
RAW_QUEUES = {
    "square_nostorage_raw": {
        "statuses_no_storage": ("started", "success"),
        "job_factory": lambda rawparam: {
            "path": "tasks.Square",
            "params": {
                "n": rawparam
            }
        }
    }
}
```

Let's try that with a single-process worker:

```
./enqueue.py square_nostorage_raw 20000 && mrq-worker square_nostorage_raw --greenlets 50
...
[INFO] Worker spent 8.449 seconds performing 20000 jobs (2367.030 jobs/second)
```

Redis should be at less than 1% CPU load, so we can definitely keep adding processes:

```
./enqueue.py square_nostorage_raw 20000 && mrq-worker square_nostorage_raw --greenlets 50 --processes 5
...
[INFO] Worker spent 11.884 seconds performing 20850 jobs (1754.444 jobs/second)
...
[INFO] Worker spent 11.851 seconds performing 20950 jobs (1767.750 jobs/second)
...
```

We are now close to 9000 jobs per second, maxing-out the local CPUs again!

From there on, the sky is the limit! You should be able to run thousands of workers accross hundreds of machines before maxing-out a high-performance Redis instance.

Beyond that, using using multiple queues on a [Redis Cluster](https://redis.io/topics/cluster-tutorial) will definitely allow you to run several million jobs per second. If you do, please drop us a line ;-)


## Choosing the right kind of queue

### Queue types

With the different settings explored in this tutorial, MRQ allows you to choose how much data you want to store in MongoDB and Redis.

By choosing the right kind of queue for your jobs, you will strike a balance between performance, visibility in the dashboard, and safety guarantees.

Here is a table to sum up the choices:

| **Queue type**                         | **Regular** | **Raw**     | **Raw with no_storage config** |
|----------------------------------------|-------------|-------------|--------------------------------|
| **Storage for queued jobs**            | MongoDB     | Redis       | Redis                          |
| **Storage for started & success jobs** | MongoDB     | MongoDB     | None                           |
| **Performance**                        | +           | ++          | +++                            |
| **Visibility in the dashboard**        | Full        | After start | Job counts & failed jobs       |
| **Safety**                             | +++         | ++          | +                              |




### Job safety

A regular queue is guaranteed not to lose any jobs once they have been inserted in MongoDB.

A raw queue can lose jobs if the worker abruptly exits in a short time window, between the dequeue from Redis and the insertion in MongoDB.

A raw queue backed by Redis only won't be able to guarantee that a job is finished once it has been dequeued, if the worker abruptly exists.

There are several ways to make raw queues safer. The easiest one is to use a `timed_set` raw queue backed by a Redis ZSET. We'll expand on this in an upcoming tutorial!
