Simple task scheduler with MRQ
===========================

This is a simple exemple of a recurring task using MRQ, printing text to the terminal.

How to use
==========

Launch MongoDB & Redis if they are not already started:
```
$ mongod &
$ redis-server &
```

Then launch a scheduler worker by feeding him the config.
```
mrq-worker --scheduler --config config.py
```

Other features
==============

We have here an exemple of a task recurring every ~10 seconds. It is also possible to launch the tasks everyday, or on specifics days of the week or month.

We advise you to look at the [tests](https://github.com/pricingassistant/mrq/blob/master/tests/test_scheduler.py) for these use-cases.