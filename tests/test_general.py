from bson import ObjectId
import urllib2
import json
import time


def test_general_simple_task_one(worker):

  result = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 1})

  assert result == 42

  time.sleep(0.1)

  db_workers = list(worker.mongodb_jobs.mrq_workers.find())
  assert len(db_workers) == 1
  assert db_workers[0]["status"] in ["full", "wait"]

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
  assert db_jobs[0]["path"] == "mrq.basetasks.tests.general.Add"
  assert db_jobs[0]["time"] < 0.1
  assert db_jobs[0]["switches"] >= 1

  db_workers = list(worker.mongodb_jobs.mrq_workers.find())
  assert len(db_workers) == 1
  assert db_workers[0]["_id"] == db_jobs[0]["worker"]
  assert db_workers[0]["status"] == "stop"
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


def test_current_job_inspect(worker):

  job_id = worker.send_task("mrq.basetasks.tests.general.MongoInsert", {"a": 41, "b": 1, "sleep": 3}, block=False)

  time.sleep(1)

  # Test the HTTP admin API
  admin_worker = json.load(urllib2.urlopen("http://localhost:20020"))

  assert admin_worker["status"] == "full"
  assert len(admin_worker["jobs"]) == 1
  assert admin_worker["jobs"][0]["mongodb"]["insert"] == 1

  # And now the $1M feature: check which function call is currently running!
  assert "sleep(" in "\n".join(admin_worker["jobs"][0]["stack"])
  assert "tests/general.py" in "\n".join(admin_worker["jobs"][0]["stack"])
  # print "STACK", "\n".join(admin_worker["jobs"][0]["stack"])

  assert admin_worker["jobs"][0]["id"] == str(job_id)


def test_general_simple_no_trace(worker):

  worker.start(trace=False)

  result = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1})

  assert result == 42


def test_general_simple_task_multiple(worker):

  result = worker.send_tasks("mrq.basetasks.tests.general.Add", [
    {"a": 41, "b": 1, "sleep": 1},
    {"a": 41, "b": 1, "sleep": 1},
    {"a": 40, "b": 1, "sleep": 1}
  ])

  assert result == [42, 42, 41]

  assert [x["result"] for x in worker.mongodb_jobs.mrq_jobs.find().sort([["dateupdated", 1]])] == [42, 42, 41]


def test_general_simple_task_reverse(worker):

  worker.start(queues="default_reverse")

  result = worker.send_tasks("mrq.basetasks.tests.general.Add", [
    {"a": 41, "b": 1, "sleep": 1},
    {"a": 41, "b": 1, "sleep": 1},
    {"a": 40, "b": 1, "sleep": 1}
  ])

  assert result == [42, 42, 41]

  assert [x["result"] for x in worker.mongodb_jobs.mrq_jobs.find().sort([["dateupdated", 1]])] == [41, 42, 42]


def test_general_exception_status(worker):

  worker.send_task("mrq.basetasks.tests.general.RaiseException", {"message": "xyz"}, block=True, accept_statuses=["failed"])

  job1 = worker.mongodb_jobs.mrq_jobs.find_one()
  assert job1
  assert job1["exceptiontype"] == "Exception"
  assert job1["status"] == "failed"
  assert "raise" in job1["traceback"]
  assert "xyz" in job1["traceback"]

