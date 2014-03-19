from bson import ObjectId
import urllib2
import json
import time


def test_general_simple_task_one(worker):

  worker.start()

  # Check that sequential sleeps work
  start_time = time.time()
  result = worker.send_task("mrq.basetasks.tests.general.SubPool", {
    "pool_size": 1, "inner_params": [1, 1]
  })
  total_time = time.time() - start_time

  assert result == [1, 1]
  assert total_time > 2

  # Parallel sleeps
  start_time = time.time()
  result = worker.send_task("mrq.basetasks.tests.general.SubPool", {
    "pool_size": 20, "inner_params": [1] * 20
  })
  total_time = time.time() - start_time

  assert result == [1] * 20
  assert total_time < 2

  # Exception
  result = worker.send_task("mrq.basetasks.tests.general.SubPool", {
    "pool_size": 20, "inner_params": ["exception"]
  }, accept_statuses=["failed"])
