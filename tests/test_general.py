from bson import ObjectId
import urllib2
import json


def test_general_simple_task_one(worker):

  result = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1})

  assert result == 42

  db_workers = list(worker.mongodb_logs.mrq_workers.find())
  assert len(db_workers) == 1
  assert db_workers[0]["status"] == "idle"

  # Test the HTTP admin API
  admin_worker = json.load(urllib2.urlopen("http://localhost:20000"))

  assert admin_worker["id"] == str(db_workers[0]["id"])
  assert admin_worker["status"] == "idle"

  # Stop the worker gracefully
  worker.stop(deps=False)

  db_jobs = list(worker.mongodb_jobs.mrq_jobs.find())
  assert len(db_jobs) == 1
  assert db_jobs[0]["result"] == 42
  assert db_jobs[0]["status"] == "success"
  assert db_jobs[0]["queue"] == "default"
  assert db_jobs[0]["worker"]
  assert db_jobs[0]["datestarted"]
  assert db_jobs[0]["_id"]
  assert db_jobs[0]["params"] == {"a": 41, "b": 1}
  assert db_jobs[0]["path"] == "mrq.basetasks.tests.general.Add"

  db_workers = list(worker.mongodb_logs.mrq_workers.find())
  assert len(db_workers) == 1
  assert db_workers[0]["name"] == db_jobs[0]["worker"]
  assert db_workers[0]["status"] == "stopping"
  assert db_workers[0]["jobs"] == []
  assert db_workers[0]["done_jobs"] == 1
  assert db_workers[0]["config"]
  assert db_workers[0]["_id"]

  # Job logs
  db_logs = list(worker.mongodb_logs.mrq_logs.find({"job": db_jobs[0]["_id"]}))
  assert len(db_logs) == 1
  assert "adding" in db_logs[0]["logs"]

  # Worker logs
  db_logs = list(worker.mongodb_logs.mrq_logs.find({"worker": db_workers[0]["_id"]}))
  assert len(db_logs) >= 1


def test_general_simple_task_multiple(worker):

  result = worker.send_tasks("mrq.basetasks.tests.general.Add", [
    {"a": 41, "b": 1},
    {"a": 41, "b": 1},
    {"a": 40, "b": 1}
  ])

  assert result == [42, 42, 41]


def test_general_exception_status(worker):

  worker.send_task("mrq.basetasks.tests.general.RaiseException", {"message": "xxx"}, block=True, accept_statuses=["failed"])


def test_general_exception_timeout(worker):

  worker.start(flags="--config tests/fixtures/config1.py")

  r = worker.send_task("mrq.basetasks.tests.general.TimeoutFromConfig", {"a": 1, "b": 2}, block=True)
  assert r == 3

  r = worker.send_task("mrq.basetasks.tests.general.TimeoutFromConfig", {"a": 1, "b": 2, "sleep": 1000}, block=True, accept_statuses=["timeout"])
  assert r != 3
