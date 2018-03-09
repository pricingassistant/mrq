Simple task scheduler with MRQ
===========================

This is a simple exemple of a recurring task using MRQ.

We have a simple task that print text to the terminal, and we want to launch it every 10 seconds. That is what this exemple is demonstrating.

How to use
==========

First, get into the docker image at the root of this directory:
```
docker run -t -i -v `pwd`:/src -w /src pricingassistant/mrq bash
```
Don't forget to `cd` in the directory of this exemple!

Launch MongoDB & Redis if they are not already started:
```
$ mongod &
$ redis-server &
```

Then launch a scheduler worker, feeding him the config.
```
mrq-worker --scheduler --config config.py
```

In the config. we have described two time the task, with different parameters
You should then see the task printing its parameters every 10 seconds on the terminal.

```
2018-03-09 11:16:03.927182 [DEBUG] Scheduler: added tasks.Print 1 None None None None [["x","Another test."]]
2018-03-09 11:16:03.930823 [DEBUG] Scheduler: added tasks.Print 1 None None None None [["x","Test."]]
2018-03-09 11:16:03.960213 [DEBUG] Scheduler: queued tasks.Print 1 None None None None [["x","Another test."]]
2018-03-09 11:16:03.970537 [DEBUG] Scheduler: queued tasks.Print 1 None None None None [["x","Test."]]
2018-03-09 11:16:04.887654 [DEBUG] Starting tasks.Print({u'x': u'Another test.'})
Hello world !
Another test.
2018-03-09 11:16:04.901856 [DEBUG] Job 5aa26cf322f9db001dfdbdb0 success: 0.014454s total
2018-03-09 11:16:04.903789 [DEBUG] Starting tasks.Print({u'x': u'Test.'})
Hello world !
Test.
2018-03-09 11:16:04.905972 [DEBUG] Job 5aa26cf322f9db001dfdbdb1 success: 0.002258s total
2018-03-09 11:16:14.985899 [DEBUG] Scheduler: queued tasks.Print 1 None None None None [["x","Another test."]]
2018-03-09 11:16:14.988513 [DEBUG] Scheduler: queued tasks.Print 1 None None None None [["x","Test."]]
2018-03-09 11:16:15.961432 [DEBUG] Starting tasks.Print({u'x': u'Another test.'})
Hello world !
Another test.
2018-03-09 11:16:15.964429 [DEBUG] Job 5aa26cfe22f9db001dfdbdb4 success: 0.004088s total
2018-03-09 11:16:15.967952 [DEBUG] Starting tasks.Print({u'x': u'Test.'})
Hello world !
Test.
2018-03-09 11:16:15.970359 [DEBUG] Job 5aa26cfe22f9db001dfdbdb5 success: 0.002809s total
```

Other features
==============

It is also possible to launch tasks everyday, or on specifics days of the week or month.

We advise you to look at the [tests](https://github.com/pricingassistant/mrq/blob/master/tests/test_scheduler.py) for these use-cases.