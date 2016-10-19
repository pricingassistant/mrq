from mrq.job import Job
from mrq.queue import Queue, send_task
import time


def test_pause_resume(worker):

    worker.start()

    Queue("high").pause()

    assert Queue("high").is_paused()

    job_id1 = send_task(
        "tests.tasks.general.MongoInsert", {"a": 41},
        queue="high")

    job_id2 = send_task(
        "tests.tasks.general.MongoInsert", {"a": 43},
        queue="low")

    time.sleep(5)

    job1 = Job(job_id1).fetch().data
    job2 = Job(job_id2).fetch().data

    assert job1["status"] == "queued"

    assert job2["status"] == "success"
    assert job2["result"] == {"a": 43}

    assert worker.mongodb_jobs.tests_inserts.count() == 1

    Queue("high").resume()

    Job(job_id1).wait(poll_interval=0.01)

    job1 = Job(job_id1).fetch().data

    assert job1["status"] == "success"
    assert job1["result"] == {"a": 41}

    assert worker.mongodb_jobs.tests_inserts.count() == 2
