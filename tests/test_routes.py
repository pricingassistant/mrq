from builtins import str
from builtins import bool
import json
import pytest


def test_routes_taskexceptions(worker, api):

    task_path = "tests.tasks.general.RaiseException"
    worker.send_task(task_path, {
                     "message": "xyz"}, block=True, accept_statuses=["failed"])

    data, _ = api.GET("/api/datatables/taskexceptions?sEcho=1")
    assert len(data["aaData"]) == 1
    assert data["aaData"][0]["_id"]["path"] == task_path


def test_routes_status(worker, api):

    worker.send_task("tests.tasks.general.RaiseException", {
                     "message": "xyz"}, block=True, accept_statuses=["failed"])

    data, _ = api.GET("/api/datatables/status?sEcho=1")
    assert len(data["aaData"]) == 1


def test_routes_taskpaths(worker, api):

    task_path = "tests.tasks.general.RaiseException"
    worker.send_task(task_path, {
                     "message": "xyz"}, block=True, accept_statuses=["failed"])

    data, _ = api.GET("/api/datatables/taskpaths?sEcho=1")

    assert len(data["aaData"]) == 1
    assert data["aaData"][0]["_id"] == task_path
    assert data["aaData"][0]["jobs"] == 1


def test_routes_workers(worker, api):

    worker.start()

    data, _ = api.GET("/workers")
    assert len(data) == 1


def test_routes_traceback(worker, api):

    worker.send_task("tests.tasks.general.RaiseException", {
                     "message": "xyz"}, block=True, accept_statuses=["failed"])

    job = worker.mongodb_jobs.mrq_jobs.find_one()
    assert job

    data, _ = api.GET("/api/job/%s/traceback" % job["_id"])
    assert "xyz" in data["traceback"]


def test_routes_result(worker, api):

    worker.send_task("tests.tasks.general.ReturnParams", {
                     "message": "xyz"}, block=True)

    job = worker.mongodb_jobs.mrq_jobs.find_one()
    assert job

    data, _ = api.GET("/api/job/%s/result" % job["_id"])
    assert data["result"]["message"] == "xyz"


def test_routes_jobaction(worker, api):

    worker.send_task("tests.tasks.general.ReturnParams", {
                     "message": "xyz"}, block=False, queue="tmp")

    job = worker.mongodb_jobs.mrq_jobs.find_one()
    assert job

    params = {
        "action": "cancel",
        "id": str(job["_id"])
    }
    data, _ = api.POST("/api/jobaction", data=json.dumps(params))

    assert "job_id" in data
    assert bool(data["job_id"])


def test_routes_datatables(worker, api):

    worker.start(flags="--config tests/fixtures/config-raw1.py")

    task_path = "tests.tasks.general.RaiseException"
    worker.send_task(task_path, {
                     "message": "xyz"}, block=True, accept_statuses=["failed"])

    unit = "workers"
    data, _ = api.GET("/api/datatables/%s?sEcho=1" % unit)
    assert len(data["aaData"]) == 1

    unit = "jobs"
    data, _ = api.GET("/api/datatables/%s?sEcho=1" % unit)
    assert len(data["aaData"]) == 1
    assert data["aaData"][0]["path"] == task_path
    assert data["aaData"][0]["status"] == "failed"
    assert "xyz" in data["aaData"][0]["traceback"]

    unit = "queues"
    data, _ = api.GET("/api/datatables/%s?sEcho=1" % unit)
    assert len(data["aaData"]) > 1

    # TODO: test unit = "scheduled_jobs"


def test_routes_logs(worker, api):

    task_path = "tests.tasks.general.RaiseException"
    worker.send_task(task_path, {
                     "message": "xyz"}, block=True, accept_statuses=["failed"])

    job = worker.mongodb_jobs.mrq_jobs.find_one()
    assert job

    data, _ = api.GET("/api/logs?job=%s" % job["_id"])

    assert bool(data["last_log_id"])
    # TODO: test actual logs
