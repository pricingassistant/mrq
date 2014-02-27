import time


def benchmark_task(worker, taskpath, taskparams, tasks=1000, greenlets=50, max_seconds=10, profile=False, quiet=True):

  worker.start(flags="-n %s%s%s" % (greenlets, " --profile" if profile else "", " --quiet" if quiet else ""))

  start_time = time.time()

  # result = worker.send_tasks("mrq.basetasks.tests.general.Add",
  #                            [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)])

  result = worker.send_tasks(taskpath, taskparams)

  total_time = time.time() - start_time

  print "%s tasks done with %s greenlets in %0.3f seconds : %0.2f jobs/second!" % (tasks, greenlets, total_time, tasks / total_time)

  assert total_time < max_seconds

  return result, total_time


def test_performance_simpleadds(worker):

  n_tasks = 10000
  n_greenlets = 50
  max_seconds = 20

  result, total_time = benchmark_task(worker,
                                      "mrq.basetasks.tests.general.Add",
                                      [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)],
                                      tasks=n_tasks,
                                      greenlets=n_greenlets,
                                      max_seconds=max_seconds)

  # ... and return correct results
  assert result == range(n_tasks)


def test_performance_httpstatic_internal(worker, httpstatic):

  httpstatic.start()

  n_tasks = 1000
  n_greenlets = 50
  max_seconds = 10

  result, total_time = benchmark_task(worker,
                                      "mrq.basetasks.tests.general.Fetch",
                                      [{"url": "http://127.0.0.1:8081/"} for _ in range(n_tasks)],
                                      tasks=n_tasks,
                                      greenlets=n_greenlets,
                                      max_seconds=max_seconds,
                                      profile=False)


def test_performance_httpstatic_external(worker):

  n_tasks = 1000
  n_greenlets = 50
  max_seconds = 25

  url = "http://www.microsoft.com/favicon.ico"
  url = "http://ox-mockserver.herokuapp.com/ipheaders"
  # url = "http://ox-mockserver.herokuapp.com/timeout?timeout=1000"

  result, total_time = benchmark_task(worker,
                                      "mrq.basetasks.tests.general.Fetch",
                                      [{"url": url} for _ in range(n_tasks)],
                                      tasks=n_tasks,
                                      greenlets=n_greenlets,
                                      max_seconds=max_seconds, quiet=False)
