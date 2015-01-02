# I/O Monitoring

One of the most powerful features of MRQ is the ability to view in the Dashboard the current state of the running jobs.

You can view their current callstack, and if the worker was started with `--trace_io`, the high-level details of the I/O operation they are waiting for, if any.

This is done through gevent-style monkey patching of common I/O modules. Until further documentation, see [monkey.py](https://github.com/pricingassistant/mrq/blob/master/mrq/monkey.py)