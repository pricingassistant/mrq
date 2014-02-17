import time


def test_performance_manytasks(worker):

  n_tasks = 10000
  n_greenlets = 50
  n_seconds = 20

  # 100 greenlets
  worker.start(flags="-n %s --quiet" % n_greenlets)  # --profile

  start_time = time.time()

  result = worker.send_tasks("mrq.basetasks.tests.general.Add",
                             [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)])

  total_time = time.time() - start_time

  # Should be done quickly!
  print "%s tasks done with %s greenlets in %0.3f seconds : %0.2f jobs/second!" % (n_tasks, n_greenlets, total_time, n_tasks / total_time)
  assert total_time < n_seconds

  # ... and return correct results
  assert result == range(n_tasks)


# TODO test with latency with http://www.linuxfoundation.org/collaborate/workgroups/networking/netem
# or a local proxy
