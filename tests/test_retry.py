from mrq.queue import wait_for_job
import datetime


def test_retry(worker):

  job_id = worker.send_task("mrq.basetasks.tests.general.Retry", {"queue": "noexec", "countdown": 60}, block=False)

  job_data = wait_for_job(job_id, poll_interval=0.01, full_data=True)

  assert job_data["queue"] == "noexec"
  assert job_data["status"] == "retry"
  assert job_data["dateretry"] > datetime.datetime.utcnow()
  assert job_data.get("result") is None
