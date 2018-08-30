# Worker performance

Performance is an explicit goal of MRQ as it was first developed at [Pricing Assistant](http://www.pricingassistant.com/) for crawling billions of web pages.

## Throughput tests

On a regular Macbook Pro, we see 1300 jobs/second in a single worker process with very simple jobs that store results, to measure the overhead of MRQ.

However what we are really measuring there is MongoDB's write performance. An install of MRQ with properly scaled MongoDB and Redis instances is be capable of much more.

For more, see our tutorial on [Queue performance](queue-performance.md).

## PyPy support

Earlier in its development MRQ was tested successfully on PyPy but we are waiting for better PyPy+gevent support to continue working on it, as performance was worse than CPython.

## Heroku

On Heroku's 1X dynos with 512M RAM, we have found that for IO-bound jobs, `--processes 4 --greenlets 30` may be a good setting.
