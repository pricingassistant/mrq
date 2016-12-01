from mrq.job import Job


def test_locked_job(worker):

  worker.start(flags="--greenlets=2")

  job_ids = []

  for i in range(4):
    job_ids.append(
      worker.send_task("tests.tasks.lock.Locked", {"queue": "default"}, block=False))

  worker.wait_for_tasks_results(job_ids, accept_statuses=["success", "failed", "expired"])
  worker.stop()

  statuses = [Job(job_id).fetch().data["status"] for job_id in job_ids]
  assert "success" in statuses
  assert "expired" in statuses
  assert "failed" not in statuses
