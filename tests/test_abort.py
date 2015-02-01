from mrq.job import Job
from mrq.queue import Queue
import time


def test_abort(worker):

    worker.start()

    worker.send_task("tests.tasks.general.Abort", {"a": 41}, accept_statuses=["abort"])

    assert Queue("default").size() == 0

    db_jobs = list(worker.mongodb_jobs.mrq_jobs.find())
    assert len(db_jobs) == 1

    assert db_jobs[0]["status"] == "abort"