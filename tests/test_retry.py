from builtins import str
from mrq.job import Job
import datetime
from mrq.queue import Queue
import time


def test_retry(worker):

    job_id = worker.send_task(
        "tests.tasks.general.Retry", {"queue": "noexec", "delay": 60}, block=False)

    job_data = Job(job_id).wait(poll_interval=0.01, full_data=True)

    assert job_data["queue"] == "noexec"
    assert job_data["status"] == "retry"
    assert job_data["dateretry"] > datetime.datetime.utcnow()
    assert job_data.get("result") is None


def test_retry_otherqueue_delay_zero(worker):

    # delay = 0 should requeue right away.
    job_id = worker.send_task(
        "tests.tasks.general.Retry", {"queue": "noexec", "delay": 0}, block=False)

    time.sleep(1)

    assert worker.mongodb_jobs.tests_inserts.find().count() == 1

    assert Queue("default").size() == 0
    assert Queue("noexec").size() == 1
    assert Queue("noexec").list_job_ids() == [str(job_id)]


def test_retry_otherqueue_delay_nonzero(worker):

    # delay = 0 should requeue right away.
    worker.send_task("tests.tasks.general.Retry", {
        "queue": "noexec",
        "delay": 2
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


def test_retry_from_other_queue_stays_on_queue(worker):
    worker.start(queues="default exec")

    worker.send_task("tests.tasks.general.Retry", {
        "delay": 1
    }, queue="exec", accept_statuses="retry")

    time.sleep(2)

    job_id = worker.mongodb_jobs.mrq_jobs.find()[0]["_id"]
    job = Job(job_id).fetch()

    assert job.data["status"] == "retry"
    assert job.data["queue"] == "exec"

    assert Queue("default").size() == 0
    assert Queue("exec").size() == 0

    worker.stop(deps=False)

    worker.start(queues="default", deps=False)

    # Should do nothing yet
    worker.send_task("mrq.basetasks.cleaning.RequeueRetryJobs", {}, block=True)

    assert Queue("default").size() == 0
    assert Queue("exec").size() == 1

    job = Job(job_id).fetch()
    assert job.data["status"] == "queued"
    assert job.data["queue"] == "exec"


def test_retry_max_retries(worker):

    # Task has maxretries=1
    worker.start(flags="--config tests/fixtures/config-retry1.py")

    worker.send_task("tests.tasks.general.Retry", {

    }, block=True, accept_statuses=["retry"])

    assert Queue("default").size() == 0

    job_id = worker.mongodb_jobs.mrq_jobs.find()[0]["_id"]

    job = Job(job_id).fetch()
    assert job.data["status"] == "retry"
    assert job.data["retry_count"] == 1

    time.sleep(2)

    # Should requeue
    worker.send_task("mrq.basetasks.cleaning.RequeueRetryJobs", {}, block=True)

    time.sleep(2)

    assert Queue("default").size() == 0

    job = Job(job_id).fetch()
    assert job.data["status"] == "maxretries"
    assert job.data["retry_count"] == 1

    # Then, manual requeue from the dashboard should reset the retry_count field.
    params = {
        "action": "requeue",
        "status": "maxretries",
        "destination_queue": "noexec"
    }

    worker.send_task("mrq.basetasks.utils.JobAction", params, block=True)

    job = Job(job_id).fetch()
    assert job.data["status"] == "queued"
    assert job.data["queue"] == "noexec"
    assert job.data["retry_count"] == 0


def test_retry_max_retries_zero(worker):

    # Task has maxretries=1
    worker.start(flags="--config tests/fixtures/config-retry1.py")

    worker.send_task("tests.tasks.general.Retry", {
        "max_retries": 0
    }, block=True, accept_statuses=["maxretries"])

    assert Queue("default").size() == 0

    job_id = worker.mongodb_jobs.mrq_jobs.find()[0]["_id"]

    job = Job(job_id).fetch()
    assert job.data["status"] == "maxretries"


def test_retry_traceback_history(worker):

    worker.start(flags="--config tests/fixtures/config-tracebackhistory.py")
    # delay = 0 should requeue right away.

    worker.send_task(
        "tests.tasks.general.Retry", {"queue": "noexec", "delay": 60}, block=True, accept_statuses=["retry"]
    )

    job = worker.mongodb_jobs.mrq_jobs.find()[0]

    assert len(job["traceback_history"]) == 1
    assert not job["traceback_history"][0].get("original_traceback")

    worker.send_task(
        "tests.tasks.general.RetryOnFailed", {"queue": "default", "delay": 1}, block=True, accept_statuses=["retry"]
    )

    job = worker.mongodb_jobs.mrq_jobs.find({
        "path": "tests.tasks.general.RetryOnFailed"})[0]

    assert len(job["traceback_history"]) == 1
    assert "InRetryException" in job["traceback_history"][0].get("original_traceback")
    time.sleep(2)
    worker.send_task("mrq.basetasks.cleaning.RequeueRetryJobs", {}, block=True)
    time.sleep(2)
    job = worker.mongodb_jobs.mrq_jobs.find({
        "path": "tests.tasks.general.RetryOnFailed"})[0]

    assert len(job["traceback_history"]) == 2
    assert job["traceback_history"][0]["date"] < job["traceback_history"][1]["date"]
