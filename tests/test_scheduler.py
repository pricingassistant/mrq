from bson import ObjectId
import urllib2
import time


def test_scheduler_simple(worker):

  worker.start(flags="--scheduler --config tests/fixtures/config-scheduler1.py")

  collection = worker.mongodb_logs.tests_inserts
  scheduled_jobs = worker.mongodb_jobs.scheduled_jobs

  time.sleep(2)

  # There are 4 test tasks with 5 second interval
  inserts = list(collection.find())
  assert len(inserts) == 4

  jobs = list(scheduled_jobs.find())
  assert len(jobs) == 4

  time.sleep(5)

  # They should have ran again.
  inserts = list(collection.find())
  assert len(inserts) == 8

  worker.stop(deps=False)

  collection.remove({})

  # Start with new config
  worker.start(deps=False, flags="--scheduler --config tests/fixtures/config-scheduler2.py")

  time.sleep(2)

  jobs2 = list(scheduled_jobs.find())
  assert len(jobs2) == 4
  assert jobs != jobs2

  # Only 3 should have been replaced and ran immediately again because they have different config.
  inserts = list(collection.find())
  assert len(inserts) == 3


def test_scheduler_dailytime(worker):

  # Task is scheduled in 3 seconds
  worker.start(flags="--scheduler --config tests/fixtures/config-scheduler3.py")

  # It will be done a first time immediately

  time.sleep(1)

  collection = worker.mongodb_logs.tests_inserts

  assert collection.find().count() == 1

  # Then a second time once the dailytime passes
  time.sleep(4)

  assert collection.find().count() == 2
