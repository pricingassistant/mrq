import time


def get_diff_after_jobs(worker, n_tasks, leak, sleep=0):

  time.sleep(3)

  mem_start = worker.get_report()["process"]["mem"]["rss"]

  worker.send_tasks(
    "mrq.basetasks.tests.general.Leak",
    [{"size": leak, "sleep": sleep} for _ in range(n_tasks)],
    queue="default",
    block=True
  )

  time.sleep(3)

  mem_stop = worker.get_report()["process"]["mem"]["rss"]

  diff = mem_stop - mem_start

  print "Memory diff for %s tasks was %s" % (n_tasks, diff)

  return diff


def test_memoryleaks_noleak(worker):

  worker.start(flags="--trace_memory --gevent 1 --mongodb_logs 0 --report_interval 10000")

  # Send it once to add to imports
  get_diff_after_jobs(worker, 10, 0)

  diff100 = get_diff_after_jobs(worker, 100, 0)

  assert worker.mongodb_jobs.mrq_jobs.count() == 100 + 10

  diff200 = get_diff_after_jobs(worker, 200, 0)

  assert worker.mongodb_jobs.mrq_jobs.count() == 200 + 100 + 10

  # Most of the tasks should have mem_diff == 0
  assert worker.mongodb_jobs.mrq_jobs.find({"memory_diff": 0}).count() > 310 * 0.95

  assert abs(diff100 - diff200) < 20000

  assert diff200 < 150000


def test_memoryleaks_1mleak(worker):

  worker.start(flags="--trace_memory --gevent 1 --mongodb_logs 0 --report_interval 10000")

  # Send it once to add to imports
  get_diff_after_jobs(worker, 10, 0)

  worker.mongodb_jobs.mrq_jobs.remove()

  # 1M leak!
  diff1m = get_diff_after_jobs(worker, 10, 100000, sleep=0.05)  # sleep is needed so that psutil measurements are accurate :-/

  assert diff1m > 900000

  assert worker.mongodb_jobs.mrq_jobs.find({"memory_diff": {"$gte": 80000}}).count() == 10
