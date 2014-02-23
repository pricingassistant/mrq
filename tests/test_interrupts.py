import time


def test_interrupt_worker_gracefully(worker):
  """ Test what happens when we interrupt a running worker gracefully. """

  worker.start()

  job_id = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 5}, block=False)

  time.sleep(1)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
  assert job["status"] == "started"

  # Stop the worker gracefully. first job should still finish!
  worker.stop(block=False, deps=False)

  time.sleep(1)

  # Should not be accepting new jobs!
  job_id2 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 42, "b": 1, "sleep": 4}, block=False)

  time.sleep(1)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id2})
  assert job.get("status") == "queued"

  time.sleep(4)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
  assert job["status"] == "success"
  assert job["result"] == 42

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id2})
  assert job.get("status") == "queued"


def test_interrupt_worker_double_sigint(worker):
  """ Test what happens when we interrupt a running worker with 2 SIGINTs. """

  start_time = time.time()

  worker.start()

  job_id = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 10}, block=False)

  time.sleep(1)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
  assert job["status"] == "started"

  # Stop the worker gracefully. first job should still finish!
  worker.stop(block=False, deps=False)

  time.sleep(1)

  # Should not be accepting new jobs!
  job_id2 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 42, "b": 1, "sleep": 10}, block=False)

  time.sleep(1)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id2})
  assert job.get("status") == "queued"

  # Sending a second kill -2 should make it stop
  worker.stop(block=False, deps=False, force=True)

  time.sleep(1)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
  assert job["status"] == "interrupt"

  assert time.time() - start_time < 8


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

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
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

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
  assert job["status"] == "started"

  assert time.time() - start_time < 5
