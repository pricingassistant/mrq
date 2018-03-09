#!/usr/bin/env python
import sys
import time
from mrq.context import setup_context
from mrq.job import queue_raw_jobs

setup_context()

queue = sys.argv[1]
n = int(sys.argv[2])
t = int(sys.argv[3])

if queue in ("example_timed_set"):
    now = time.time()
    # every 10 seconds
    queue_raw_jobs(queue, {"task_%s" % _: now + (_ + 1) * t for _ in range(n)})
