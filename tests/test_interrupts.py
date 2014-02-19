import time


def test_interrupt_worker_gracefully(worker):
  """ Test what happens when we interrupt a running worker gracefully. """

  worker.start()

  job_id = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 5}, block=False)

  time.sleep(1)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
  assert job["status"] == "started"

  # Stop the worker gracefully. first job should still finish!
  worker.stop(block=False)

  time.sleep(1)

  # Should not be accepting new jobs!
  job_id2 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 42, "b": 1, "sleep": 4}, block=False)

  time.sleep(1)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id2})
  assert job.get("status") is None

  time.sleep(4)

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id})
  assert job["status"] == "success"
  assert job["result"] == 42

  job = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id2})
  assert job.get("status") is None
