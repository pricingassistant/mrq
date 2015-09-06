from mrq.job import Job
import datetime
from mrq.queue import Queue
import time
import pytest


def test_sorted_graph(worker):

    p_queue = "test_sorted_set"

    worker.start_deps()

    assert Queue(p_queue).size() == 0

    worker.send_raw_tasks(p_queue, {
        "000": -1,
        "aaa": 1,
        "aaa2": 1.5,
        "bbb": 2,
        "ccc": 4
    }, start=False, block=False)
    time.sleep(0.5)

    assert Queue(p_queue).size() == 5
    assert Queue(p_queue).get_sorted_graph(
        1, 4, slices=3, include_inf=True) == [1, 2, 1, 0, 1]
    assert Queue(p_queue).get_sorted_graph(
        1, 4, slices=3, include_inf=False) == [2, 1, 0]

    worker.stop_deps()


# def test_sorted_graph_dashboardtest(worker):

#   p_queue = "test_timed_set"

#   worker.start_deps()

#   assert Queue(p_queue).size() == 0

#   now = time.time()

#   worker.send_raw_tasks(p_queue, {
#     "000": now - 3600 * 6,
#     "aaa": now,
#     "aaa2": now + 3600 * 12,
#     "bbb": now + 3600000,
#     "ccc": now
#   }, start=False)
#   time.sleep(10000)

#   worker.stop_deps()
