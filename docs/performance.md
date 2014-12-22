# Performance

## throughput tests

On a MacbookPro, we see 1300 jobs/second in a single worker process with very simple jobs that store results, to measure the overhead of MRQ. However what we are really measuring there is MongoDB's write performance.

## PyPy support

Earlier in its development MRQ was tested successfully on PyPy but we are waiting for better PyPy+gevent support to continue working on it, as performance was worse than CPython.