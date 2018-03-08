Simple Web Crawler with MRQ
===========================

This is a simple demo app that uses raw timed set queues.


How to use
==========

First, get into a Python virtualenv (`make virtualenv`) or into the docker image at the root of this directory (`make ssh`)

Then install MRQ and the packages needed for this example:
```
$ cd examples/timed_set
```

Launch MongoDB & Redis if they are not already started:
```
$ mongod &
$ redis-server &
```

Enqueue raw jobs with python script:

```
$ python enqueue_raw_jobs.py example_timed_set 4 10
```

You can check redis entries:

```
$ redis-cli
$ ZRANGE mrq:q:example_timed_set 0 -1 WITHSCORES
```

You should see the following lines : (except timestamp of course)

```
1) "task_0"
2) "1520457493.2588561"
3) "task_1"
4) "1520457503.2588561"
5) "task_2"
6) "1520457513.2588561"
7) "task_3"
8) "1520457523.2588561"
```

You should also launch a dashboard to monitor the progress:
```
$ mrq-dashboard
```

Then spawn a worker listenig to your timed_set queue example_timed_set:
```
$ mrq-worker example_timed_set --config=/app/examples/timed_set/config.py
```

This is obviously a very simple example, production systems will be much more complex but it gives you an overview of timed set queues and a good starting point.


Expected result for mrq-worker example_timed_set --config=/app/examples/timed_set/config.py
==================================

```
[DEBUG] Starting example.Print({'test': 'task_0'})
Hello World
Given params test is task_0
Bye

[DEBUG] Starting example.Print({'test': 'task_1'})
Hello World
Time of last tasks execution 10.04 seconds
Given params test is task_1
Bye

[DEBUG] Starting example.Print({'test': 'task_2'})
Hello World
Time of last tasks execution 10.03 seconds
Given params test is task_2
Bye

[DEBUG] Starting example.Print({'test': 'task_3'})
Hello World
Time of last tasks execution 10.03 seconds
Given params test is task_3
Bye
```

Limitation
==================================

Note that the duration beetween enqueued tasks execution depends on when you start your worker. For example if you enqueue tasks every 10 seconds from now and you're waiting 20 seconds before spawning you worker, first 2 tasks we'll be executed directly as the expected execution time is in the past. Here is the expected output of that case :

```
[DEBUG] Starting example.Print({'test': 'task_0'})
Hello World
Given params test is task_0
Bye

[DEBUG] Starting example.Print({'test': 'task_1'})
Hello World
Last task was executed 3.13 seconds ago
Given params test is task_1
Bye

[DEBUG] Starting example.Print({'test': 'task_2'})
Hello World
Last task was executed 10.03 seconds ago
Given params test is task_2
Bye

[DEBUG] Starting example.Print({'test': 'task_3'})
Hello World
Last task was executed 10.03 seconds ago
Given params test is task_3
Bye
```

