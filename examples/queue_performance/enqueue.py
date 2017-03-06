#!/usr/bin/env python
import sys
from mrq.context import setup_context
from mrq.job import queue_jobs, queue_raw_jobs

setup_context()

queue = sys.argv[1]
n = int(sys.argv[2])

if queue == "square":
    queue_jobs("tasks.Square", [{"n": 42} for _ in range(n)], queue=queue)

elif queue in ("square_raw", "square_nostorage_raw"):
    queue_raw_jobs(queue, [42 for _ in range(n)])
