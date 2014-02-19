from bson import ObjectId


def test_general_simple_task_one(worker):

  result = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1})

  assert result == 42


def test_general_simple_task_multiple(worker):

  result = worker.send_tasks("mrq.basetasks.tests.general.Add", [
    {"a": 41, "b": 1},
    {"a": 41, "b": 1},
    {"a": 40, "b": 1}
  ])

  assert result == [42, 42, 41]


def test_general_exception_status(worker):

  worker.send_task("mrq.basetasks.tests.general.RaiseException", {"message": "xxx"}, block=True, accept_statuses=["failed"])
