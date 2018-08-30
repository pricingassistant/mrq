from mrq.job import Job
import pytest
from mrq.queue import Queue, send_task
import time
from mrq.context import set_current_config, get_config


def test_pause_resume(worker):

    worker.start(flags="--paused_queues_refresh_interval=0.1")

    Queue("high").pause()

    assert Queue("high").is_paused()

    # wait for the paused_queues list to be refreshed
    time.sleep(2)

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


def test_pause_refresh_interval(worker):

    """ Tests that a refresh interval of 0 disables the pause functionnality """

    worker.start(flags="--paused_queues_refresh_interval=0")

    Queue("high").pause()

    assert Queue("high").is_paused()

    # wait for the paused_queues list to be refreshed
    time.sleep(2)

    job_id1 = send_task(
        "tests.tasks.general.MongoInsert", {"a": 41},
        queue="high")

    time.sleep(5)

    job1 = Job(job_id1).fetch().data

    assert job1["status"] == "success"
    assert job1["result"] == {"a": 41}


def test_pause_subqueue(worker):

    # set config in current context in order to have a subqueue delimiter
    set_current_config(get_config(config_type="worker"))

    worker.start(queues="high high/", flags="--subqueues_refresh_interval=1 --paused_queues_refresh_interval=1")

    Queue("high").pause()

    assert Queue("high/").is_paused()

    # wait for the paused_queues list to be refreshed
    time.sleep(2)

    job_id1 = send_task(
        "tests.tasks.general.MongoInsert", {"a": 41},
        queue="high")

    job_id2 = send_task(
        "tests.tasks.general.MongoInsert", {"a": 43},
        queue="high/subqueue")

    # wait a bit to make sure the jobs status will still be queued
    time.sleep(5)

    job1 = Job(job_id1).fetch().data
    job2 = Job(job_id2).fetch().data

    assert job1["status"] == "queued"
    assert job2["status"] == "queued"

    assert worker.mongodb_jobs.tests_inserts.count() == 0

    Queue("high/").resume()

    Job(job_id1).wait(poll_interval=0.01)

    Job(job_id2).wait(poll_interval=0.01)

    job1 = Job(job_id1).fetch().data
    job2 = Job(job_id2).fetch().data

    assert job1["status"] == "success"
    assert job1["result"] == {"a": 41}

    assert job2["status"] == "success"
    assert job2["result"] == {"a": 43}

    assert worker.mongodb_jobs.tests_inserts.count() == 2
