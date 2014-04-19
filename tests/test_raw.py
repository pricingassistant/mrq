from mrq.job import Job
import datetime
from mrq.queue import Queue
import time
import pytest


@pytest.mark.parametrize(["p_queue", "p_pushback", "p_timed", "p_flags"], [
  ["test_timed_set", False, True, "--gevent 10"],
  ["pushback_timed_set", True, True, "--gevent 10"],
  ["test_sorted_set", False, False, "--gevent 1"]
])
def test_raw_sorted(worker, p_queue, p_pushback, p_timed, p_flags):

  worker.start(flags="%s --config tests/fixtures/config-raw1.py" % p_flags, queues=p_queue)

  test_collection = worker.mongodb_logs.tests_inserts
  jobs_collection = worker.mongodb_jobs.mrq_jobs

  current_time = int(time.time())

  assert jobs_collection.count() == 0

  assert Queue(p_queue).size() == 0

  # Schedule one in the past, one in the future
  worker.send_raw_tasks(p_queue, {
    "aaa": current_time - 10,
    "bbb": current_time + 2,
    "ccc": current_time + 5
  })

  # Re-schedule
  worker.send_raw_tasks(p_queue, {
    "ccc": current_time + 2
  })

  time.sleep(1)

  if not p_timed:

    assert Queue(p_queue).size() == 0
    assert test_collection.count() == 3
    assert list(test_collection.find(fields={"params": 1, "_id": 0}).limit(1)) == [
      {"params": {"sorted_set": "aaa"}}
    ]
    return

  if p_pushback:
    assert Queue(p_queue).size() == 3
  else:
    assert Queue(p_queue).size() == 2

  # The second one should not yet even exist in mrq_jobs
  assert jobs_collection.count() == 1
  assert list(jobs_collection.find())[0]["status"] == "success"

  assert list(test_collection.find(fields={"params": 1, "_id": 0})) == [
    {"params": {"timed_set": "aaa"}}
  ]

  # Then wait for the second job to be done
  time.sleep(2)

  if p_pushback:
    assert Queue(p_queue).size() == 3
  else:
    assert Queue(p_queue).size() == 0

  assert jobs_collection.count() == 3
  assert list(jobs_collection.find())[1]["status"] == "success"
  assert list(jobs_collection.find())[2]["status"] == "success"

  assert test_collection.count() == 3


@pytest.mark.parametrize(["p_queue", "p_set"], [
  ["test_raw", False],
  ["test_set", True]
])
def test_raw_set(worker, p_queue, p_set):

  worker.start(flags="--gevent 10 --config tests/fixtures/config-raw1.py", queues=p_queue)

  test_collection = worker.mongodb_logs.tests_inserts
  jobs_collection = worker.mongodb_jobs.mrq_jobs

  assert jobs_collection.count() == 0

  assert Queue(p_queue).size() == 0

  # Schedule one in the past, one in the future
  worker.send_raw_tasks(p_queue, ["aaa", "bbb", "ccc", "bbb"])

  time.sleep(1)

  if p_set:
    assert test_collection.count() == 3

  else:
    assert test_collection.count() == 4


def test_raw_exception(worker):

  p_queue = "testexception_raw"

  worker.start(flags="--gevent 10 --config tests/fixtures/config-raw1.py", queues=p_queue)

  jobs_collection = worker.mongodb_jobs.mrq_jobs
  assert jobs_collection.count() == 0
  assert Queue(p_queue).size() == 0

  worker.send_raw_tasks(p_queue, ["msg1"])

  time.sleep(1)

  failjob = list(jobs_collection.find())[0]

  assert Queue("default").size() == 0
  assert Queue(p_queue).size() == 0
  assert jobs_collection.count() == 1
  assert failjob["status"] == "failed"

  worker.stop(deps=False)

  worker.start(deps=False, flags="--gevent 10 --config tests/fixtures/config-raw1.py", queues="default")

  worker.send_task(
    "mrq.basetasks.utils.JobAction",
    {
      "id": failjob["_id"],
      "action": "requeue"
    },
    block=True
  )

  assert Queue("default").size() == 0
  assert Queue(p_queue).size() == 0
  assert Queue("testx").size() == 1
  assert jobs_collection.count() == 2
  assert list(jobs_collection.find({"_id": failjob["_id"]}))[0]["status"] == "queued"
  assert list(jobs_collection.find({"_id": {"$ne": failjob["_id"]}}))[0]["status"] == "success"

  worker.stop(deps=False)

  worker.start(deps=False, flags="--gevent 10 --config tests/fixtures/config-raw1.py", queues="default testx")

  time.sleep(1)

  assert Queue(p_queue).size() == 0
  assert jobs_collection.count() == 2
  assert Queue("testx").size() == 0
  assert list(jobs_collection.find({"_id": failjob["_id"]}))[0]["status"] == "failed"


def test_raw_retry(worker):

  p_queue = "testretry_raw"

  worker.start(flags="--gevent 10 --config tests/fixtures/config-raw1.py", queues=p_queue)

  jobs_collection = worker.mongodb_jobs.mrq_jobs
  assert jobs_collection.count() == 0
  assert Queue(p_queue).size() == 0

  worker.send_raw_tasks(p_queue, [0])

  time.sleep(1)

  failjob = list(jobs_collection.find())[0]

  assert Queue("testx").size() == 1
  assert Queue("default").size() == 0
  assert Queue(p_queue).size() == 0
  assert jobs_collection.count() == 1
  assert failjob["status"] == "queued"
