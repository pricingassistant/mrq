from bson import ObjectId
import urllib2
import json
import time


def test_general_simple_task_one(worker):

    result = worker.send_task(
        "tests.tasks.general.Add", {"a": 41, "b": 1, "sleep": 1})

    assert result == 42

    time.sleep(0.1)

    db_workers = list(worker.mongodb_jobs.mrq_workers.find())
    assert len(db_workers) == 1
    worker_report = worker.get_report()
    assert worker_report["status"] in ["full", "wait"]
    assert worker_report["done_jobs"] == 1

    # Test the HTTP admin API
    admin_worker = json.load(urllib2.urlopen("http://localhost:20020"))

    assert admin_worker["_id"] == str(db_workers[0]["_id"])
    assert admin_worker["status"] == "wait"

    # Stop the worker gracefully
    worker.stop(deps=False)

    db_jobs = list(worker.mongodb_jobs.mrq_jobs.find())
    assert len(db_jobs) == 1
    assert db_jobs[0]["result"] == 42
    assert db_jobs[0]["status"] == "success"
    assert db_jobs[0]["queue"] == "default"
    assert db_jobs[0]["worker"]
    assert db_jobs[0]["datestarted"]
    assert db_jobs[0]["dateupdated"]
    assert db_jobs[0]["totaltime"] > 1
    assert db_jobs[0]["_id"]
    assert db_jobs[0]["params"] == {"a": 41, "b": 1, "sleep": 1}
    assert db_jobs[0]["path"] == "tests.tasks.general.Add"
    assert db_jobs[0]["time"] < 0.5
    assert db_jobs[0]["switches"] >= 1

    from mrq.job import get_job_result
    assert get_job_result(db_jobs[0]["_id"]) == {"result": 42, "status": "success"}

    db_workers = list(worker.mongodb_jobs.mrq_workers.find())
    assert len(db_workers) == 1
    assert db_workers[0]["_id"] == db_jobs[0]["worker"]
    assert db_workers[0]["status"] == "stop"
    assert db_workers[0]["jobs"] == []
    assert db_workers[0]["done_jobs"] == 1
    assert db_workers[0]["config"]
    assert db_workers[0]["_id"]

    # Job logs
    db_logs = list(
        worker.mongodb_logs.mrq_logs.find({"job": db_jobs[0]["_id"]}))
    assert len(db_logs) == 1
    assert "adding" in db_logs[0]["logs"]

    # Worker logs
    db_logs = list(
        worker.mongodb_logs.mrq_logs.find({"worker": db_workers[0]["_id"]}))
    assert len(db_logs) >= 1

    worker.stop_deps()


def test_general_nologs(worker):

    worker.start(flags="--mongodb_logs=0")

    assert worker.send_task(
        "tests.tasks.general.Add", {"a": 41, "b": 1, "sleep": 1}
    ) == 42

    db_workers = list(worker.mongodb_jobs.mrq_workers.find())
    assert len(db_workers) == 1

    # Worker logs
    db_logs = list(
        worker.mongodb_logs.mrq_logs.find({"worker": db_workers[0]["_id"]}))
    assert len(db_logs) == 0


def test_general_simple_no_trace(worker):

    worker.start(trace=False)

    result = worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1})

    assert result == 42


def test_general_simple_task_multiple(worker):

    result = worker.send_tasks("tests.tasks.general.Add", [
        {"a": 41, "b": 1, "sleep": 1},
        {"a": 41, "b": 1, "sleep": 1},
        {"a": 40, "b": 1, "sleep": 1}
    ])

    assert result == [42, 42, 41]

    assert [x["result"] for x in worker.mongodb_jobs.mrq_jobs.find().sort(
        [["dateupdated", 1]])] == [42, 42, 41]


def test_general_simple_task_reverse(worker):

    worker.start(queues="default_reverse xtest test_timed_set", flags="--config tests/fixtures/config-raw1.py")

    result = worker.send_tasks("tests.tasks.general.Add", [
        {"a": 41, "b": 1, "sleep": 1},
        {"a": 41, "b": 1, "sleep": 1},
        {"a": 40, "b": 1, "sleep": 1}
    ])

    assert result == [42, 42, 41]

    assert [x["result"] for x in worker.mongodb_jobs.mrq_jobs.find().sort(
        [["dateupdated", 1]])] == [41, 42, 42]

    # Test known queues
    from mrq.queue import Queue, send_task
    assert Queue.redis_known_queues() == set(["default", "xtest", "test_timed_set"])

    # Try queueing a task
    send_task("tests.tasks.general.Add", {"a": 41, "b": 1, "sleep": 1}, queue="x")
    time.sleep(1)
    assert Queue.redis_known_queues() == set(["x", "default", "xtest", "test_timed_set"])


def test_general_exception_status(worker):

    worker.send_task("tests.tasks.general.RaiseException", {
                     "message": "xyz"}, block=True, accept_statuses=["failed"])

    job1 = worker.mongodb_jobs.mrq_jobs.find_one()
    assert job1
    assert job1["exceptiontype"] == "Exception"
    assert job1["status"] == "failed"
    assert "raise" in job1["traceback"]
    assert "xyz" in job1["traceback"]

