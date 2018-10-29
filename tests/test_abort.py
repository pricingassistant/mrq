from datetime import datetime
from datetime import timedelta

from mrq.queue import Queue


def test_abort(worker):

    worker.start()

    worker.send_task("tests.tasks.general.Abort", {"a": 41}, accept_statuses=["abort"])

    assert Queue("default").size() == 0

    db_jobs = list(worker.mongodb_jobs.mrq_jobs.find())
    assert len(db_jobs) == 1

    job = db_jobs[0]
    assert job["status"] == "abort"
    assert job.get("dateexpires") is not None
    assert job["dateexpires"] < datetime.utcnow() + timedelta(hours=24)


def test_abort_traceback_history(worker):

    worker.start()

    worker.send_task("tests.tasks.general.Abort", {"a": 41}, block=True, accept_statuses=["abort"])

    job = worker.mongodb_jobs.mrq_jobs.find()[0]

    assert len(job["traceback_history"]) == 1
    assert not job["traceback_history"][0].get("original_traceback")

    worker.send_task("tests.tasks.general.AbortOnFailed", {"a": 41}, block=True, accept_statuses=["abort"])

    job = worker.mongodb_jobs.mrq_jobs.find({"path": "tests.tasks.general.AbortOnFailed"})[0]

    assert len(job["traceback_history"]) == 1
    assert "InAbortException" in job["traceback_history"][0].get("original_traceback")
