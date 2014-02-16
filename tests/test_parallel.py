import time


def test_parallel_100sleeps(worker):

  worker.start(flags="-n 50")

  start_time = time.time()

  # This will sleep a total of 100 seconds
  result = worker.send_tasks("mrq.basetasks.tests.general.Add", [{"a": i, "b": 0, "sleep": 1} for i in range(100)])

  total_time = time.time() - start_time

  # But should be done quickly!
  assert total_time < 10

  # ... and return correct results
  assert result == range(100)
