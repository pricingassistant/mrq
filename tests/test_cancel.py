from mrq.queue import wait_for_job


def test_cancel_by_path(worker):

  # Start the worker with only one greenlet so that tasks execute sequentially
  worker.start(flags="-n 1")

  job_id1 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 2}, block=False)

  worker.send_task("mrq.basetasks.utils.JobAction", {
    "path": "mrq.basetasks.tests.general.Add",
    "status": "queued",
    "action": "cancel"
  }, block=False)

  job_id2 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 2}, block=False)

  wait_for_job(job_id2)
  worker.stop(deps=False)

  job1 = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id1})
  job2 = worker.mongodb_jobs.mrq_jobs.find_one({"_id": job_id2})

  assert job1["status"] == "success"
  assert job1["result"] == 42

  assert job2["status"] == "cancel"
  assert job2.get("result") is None
