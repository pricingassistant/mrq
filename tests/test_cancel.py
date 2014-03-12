from mrq.job import Job


def test_cancel_by_path(worker):

  # Start the worker with only one greenlet so that tasks execute sequentially
  worker.start(flags="--gevent 1")

  job_id1 = worker.send_task("mrq.basetasks.tests.general.MongoInsert", {"a": 41, "sleep": 2}, block=False)

  worker.send_task("mrq.basetasks.utils.JobAction", {
    "path": "mrq.basetasks.tests.general.Add",
    "status": "queued",
    "action": "cancel"
  }, block=False)

  job_id2 = worker.send_task("mrq.basetasks.tests.general.MongoInsert", {"a": 43}, block=False)

  Job(job_id2).wait(poll_interval=0.01)
  worker.stop(deps=False)

  job1 = Job(job_id1).fetch().data
  job2 = Job(job_id2).fetch().data

  assert job1["status"] == "success"
  assert job1["result"] == {"a": 41, "sleep": 2}

  assert job2["status"] == "cancel"
  assert job2.get("result") is None

  assert worker.mongodb_logs.test_inserts.count() == 1
