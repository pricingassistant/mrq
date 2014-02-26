import time, datetime
from mrq.job import Job
from mrq.queue import Queue
from bson import ObjectId


def test_interrupt_worker_gracefully(worker):
  """ Test what happens when we interrupt a running worker gracefully. """

  worker.start()

  job_id = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 5}, block=False)

  time.sleep(1)

  job = Job(job_id).fetch().data
  assert job["status"] == "started"

  # Stop the worker gracefully. first job should still finish!
  worker.stop(block=False, deps=False)

  time.sleep(1)

  # Should not be accepting new jobs!
  job_id2 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 42, "b": 1, "sleep": 4}, block=False)

  time.sleep(1)

  job = Job(job_id2).fetch().data
  assert job.get("status") == "queued"

  time.sleep(4)

  job = Job(job_id).fetch().data
  assert job["status"] == "success"
  assert job["result"] == 42

  job = Job(job_id2).fetch().data
  assert job.get("status") == "queued"


def test_interrupt_worker_double_sigint(worker):
  """ Test what happens when we interrupt a running worker with 2 SIGINTs. """

  start_time = time.time()

  worker.start()

  job_id = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 10}, block=False)

  time.sleep(1)

  job = Job(job_id).fetch().data
  assert job["status"] == "started"

  # Stop the worker gracefully. first job should still finish!
  worker.stop(block=False, deps=False)

  time.sleep(1)

  # Should not be accepting new jobs!
  job_id2 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 42, "b": 1, "sleep": 10}, block=False)

  time.sleep(1)

  job = Job(job_id2).fetch().data
  assert job.get("status") == "queued"

  # Sending a second kill -2 should make it stop
  worker.stop(block=True, deps=False, force=True)

  time.sleep(1)

  job = Job(job_id).fetch().data
  assert job["status"] == "interrupt"

  assert time.time() - start_time < 8

  # Then try the cleaning task that requeues interrupted jobs

  assert Queue("default").size() == 1

  worker.start(queues="cleaning", deps=False, reset=False)

  res = worker.send_task("mrq.basetasks.cleaning.RequeueInterruptedJobs", {}, block=True, queue="cleaning")

  assert res["requeued"] == 1

  assert Queue("default").size() == 2

  Queue("default").list_job_ids() == [str(job_id2), str(job_id)]

  job = Job(job_id).fetch().data
  assert job["status"] == "queued"
  assert job["queue"] == "default"


def test_interrupt_worker_sigterm(worker):
  """ Test what happens when we interrupt a running worker with 1 SIGTERM.

      We should have had time to mark the task as 'interrupt' so that we can restart it somewhere else right away.
  """

  start_time = time.time()

  worker.start()

  job_id = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 10}, block=False)

  time.sleep(1)

  worker.stop(block=True, sig=15, deps=False)

  time.sleep(1)

  job = Job(job_id).fetch().data
  assert job["status"] == "interrupt"

  assert time.time() - start_time < 5


def test_interrupt_worker_sigkill(worker):
  """ Test what happens when we interrupt a running worker with 1 SIGKILL.

      SIGKILLs can't be intercepted by the process so the job should still be in 'started' state.
  """

  start_time = time.time()

  worker.start()

  job_id = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 10}, block=False)

  time.sleep(1)

  worker.stop(block=True, sig=9, deps=False)

  time.sleep(1)

  job = Job(job_id).fetch().data
  assert job["status"] == "started"

  assert time.time() - start_time < 5

  # Then try the cleaning task that requeues started jobs

  # We need to fake the datestarted
  worker.mongodb_jobs.mrq_jobs.update({"_id": ObjectId(job_id)}, {"$set": {
    "datestarted": datetime.datetime.utcnow() - datetime.timedelta(seconds=400)
  }})

  assert Queue("default").size() == 0

  worker.start(queues="cleaning", deps=False, reset=False)

  res = worker.send_task("mrq.basetasks.cleaning.RequeueStartedJobs", {"timeout": 110}, block=True, queue="cleaning")

  assert res["requeued"] == 0
  assert res["started"] == 2  # current job should count too

  assert Queue("default").size() == 0

  job = Job(job_id).fetch().data
  assert job["status"] == "started"
  assert job["queue"] == "default"

  # Now do it again with a small enough timeout
  res = worker.send_task("mrq.basetasks.cleaning.RequeueStartedJobs", {"timeout": 90}, block=True, queue="cleaning")

  assert res["requeued"] == 1
  assert res["started"] == 2  # current job should count too
  assert Queue("default").size() == 1

  Queue("default").list_job_ids() == [str(job_id)]

  job = Job(job_id).fetch().data
  assert job["status"] == "queued"
  assert job["queue"] == "default"

