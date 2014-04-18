from mrq.job import Job
import datetime
from mrq.queue import Queue
import time


def test_raw_timed(worker):

  worker.start(flags="--gevent 10 --config tests/fixtures/config-raw1.py", queues="test.timed")

  test_collection = worker.mongodb_logs.tests_inserts
  jobs_collection = worker.mongodb_jobs.mrq_jobs

  current_time = int(time.time())

  assert jobs_collection.count() == 0

  # Schedule one in the past, one in the future
  worker.send_raw_tasks("test.timed", {
    "aaa": current_time - 10,
    "bbb": current_time + 2,
    "ccc": current_time + 2
  }, block=False)

  time.sleep(1)

  # The second one should not yet even exist in mrq_jobs
  assert jobs_collection.count() == 1
  assert list(jobs_collection.find())[0]["status"] == "success"

  assert list(test_collection.find(fields={"params": 1, "_id": 0})) == [
    {"params": {"rawp": "aaa"}}
  ]

  # Then wait for the second job to be done
  time.sleep(2)

  assert jobs_collection.count() == 3
  assert list(jobs_collection.find())[1]["status"] == "success"
  assert list(jobs_collection.find())[2]["status"] == "success"

  assert test_collection.count() == 3
