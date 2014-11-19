from mrq.job import Job
import datetime
from mrq.queue import Queue
import time


def test_retry(worker):

    job_id = worker.send_task(
        "tests.tasks.general.Retry", {"queue": "noexec", "countdown": 60}, block=False)

    job_data = Job(job_id).wait(poll_interval=0.01, full_data=True)

    assert job_data["queue"] == "noexec"
    assert job_data["status"] == "retry"
    assert job_data["dateretry"] > datetime.datetime.utcnow()
    assert job_data.get("result") is None


def test_retry_otherqueue_countdown_zero(worker):

    worker.start()

    # countdown = 0 should requeue right away.
    job_id = worker.send_task(
        "tests.tasks.general.Retry", {"queue": "noexec", "countdown": 0}, block=False)

    time.sleep(1)

    assert worker.mongodb_logs.tests_inserts.find().count() == 1

    assert Queue("default").size() == 0
    assert Queue("noexec").size() == 1
    assert Queue("noexec").list_job_ids() == [str(job_id)]


def test_retry_otherqueue_countdown_nonzero(worker):

    worker.start()

    # countdown = 0 should requeue right away.
    worker.send_task("tests.tasks.general.Retry", {
        "queue": "noexec",
        "countdown": 2
    }, block=True, accept_statuses=["retry"])

    assert Queue("default").size() == 0
    assert Queue("noexec").size() == 0

    job_id = worker.mongodb_jobs.mrq_jobs.find()[0]["_id"]

    job = Job(job_id).fetch()
    assert job.data["status"] == "retry"

    # Should do nothing yet
    worker.send_task("mrq.basetasks.cleaning.RequeueRetryJobs", {}, block=True)

    assert Queue("default").size() == 0
    assert Queue("noexec").size() == 0

    time.sleep(2)

    # Should requeue
    worker.send_task("mrq.basetasks.cleaning.RequeueRetryJobs", {}, block=True)

    assert Queue("default").size() == 0
    assert Queue("noexec").size() == 1

    job = Job(job_id).fetch()
    assert job.data["status"] == "queued"


def test_retry_cancel_on_retry(worker):

    job_id = worker.send_task("tests.tasks.general.Retry", {
        "queue": "noexec",
        "countdown": 60,
        "cancel_on_retry": True
    }, block=False)

    job_data = Job(job_id).wait(poll_interval=0.01, full_data=True)

    assert job_data["status"] == "cancel"
    assert job_data["queue"] == "default"
    assert job_data.get("result") is None
