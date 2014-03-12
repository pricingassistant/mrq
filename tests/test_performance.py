import time
from mrq.queue import Queue


def benchmark_task(worker, taskpath, taskparams, tasks=1000, greenlets=50, processes=0, max_seconds=10, profile=False, quiet=True):

  worker.start(flags="--processes %s --gevent %s%s%s" % (processes, greenlets, " --profile" if profile else "", " --quiet" if quiet else ""))

  start_time = time.time()

  # result = worker.send_tasks("mrq.basetasks.tests.general.Add",
  #                            [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)])

  result = worker.send_tasks(taskpath, taskparams)

  total_time = time.time() - start_time

  print "%s tasks done with %s greenlets and %s processes in %0.3f seconds : %0.2f jobs/second!" % (tasks, greenlets, processes, total_time, tasks / total_time)

  assert total_time < max_seconds

  return result, total_time


def test_performance_simpleadds(worker):

  n_tasks = 10000
  n_greenlets = 50
  n_processes = 0
  max_seconds = 25

  result, total_time = benchmark_task(worker,
                                      "mrq.basetasks.tests.general.Add",
                                      [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)],
                                      tasks=n_tasks,
                                      greenlets=n_greenlets,
                                      processes=n_processes,
                                      max_seconds=max_seconds)

  # ... and return correct results
  assert result == range(n_tasks)


# TODO add network latency
def test_performance_httpstatic_fast(worker, httpstatic):

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


# def test_performance_httpstatic_external(worker):

#   n_tasks = 1000
#   n_greenlets = 100
#   max_seconds = 25

#   url = "http://bing.com/favicon.ico"
#   # url = "http://ox-mockserver.herokuapp.com/ipheaders"
#   # url = "http://ox-mockserver.herokuapp.com/timeout?timeout=1000"

#   result, total_time = benchmark_task(worker,
#                                       "mrq.basetasks.tests.general.Fetch",
#                                       [{"url": url} for _ in range(n_tasks)],
#                                       tasks=n_tasks,
#                                       greenlets=n_greenlets,
#                                       max_seconds=max_seconds, quiet=False)


def test_performance_queue_cancel_requeue(worker):

  worker.start()

  n_tasks = 10000

  start_time = time.time()

  worker.send_tasks(
    "mrq.basetasks.tests.general.Add",
    [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)],
    queue="noexec",
    block=False
  )

  queue_time = time.time() - start_time

  print "Queued %s tasks in %s seconds" % (n_tasks, queue_time)
  assert queue_time < 5

  assert Queue("noexec").size() == n_tasks
  assert worker.mongodb_jobs.mrq_jobs.count() == n_tasks

  # Then cancel them all
  start_time = time.time()

  worker.send_task(
    "mrq.basetasks.utils.JobAction",
    {"queue": "noexec", "action": "cancel"},
    block=True
  )
  queue_time = time.time() - start_time
  print "Cancelled %s tasks in %s seconds" % (n_tasks, queue_time)
  assert queue_time < 10

  # Then requeue them all
  start_time = time.time()

  worker.send_task(
    "mrq.basetasks.utils.JobAction",
    {"queue": "noexec", "action": "requeue"},
    block=True
  )
  queue_time = time.time() - start_time
  print "Requeued %s tasks in %s seconds" % (n_tasks, queue_time)
  assert queue_time < 15





