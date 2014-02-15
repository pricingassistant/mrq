
def test_simple_task(worker):

  result = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1})

  assert result == 42
