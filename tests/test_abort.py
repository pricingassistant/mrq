from mrq.job import Job
from mrq.queue import Queue
from datetime import datetime
from datetime import timedelta


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
