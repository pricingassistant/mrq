import time


def test_max_memory_restart(worker):

    N = 20

    worker.start(
        flags="--processes 1 --greenlets 1 --max_memory 50 --report_interval 1")

    worker.send_tasks(
        "tests.tasks.general.Leak",
        [{"size": 1000000, "sleep": 1} for _ in range(N)],
        queue="default",
        block=True
    )

    assert worker.mongodb_jobs.mrq_jobs.find(
        {"status": "success"}).count() == N

    # We must have been restarted at least once.
    assert worker.mongodb_jobs.mrq_workers.find().count() > 1


def get_diff_after_jobs(worker, n_tasks, leak, sleep=0):

    time.sleep(3)

    mem_start = worker.get_report(with_memory=True)["process"]["mem"]["total"]

    worker.send_tasks(
        "tests.tasks.general.Leak",
        [{"size": leak, "sleep": sleep} for _ in range(n_tasks)],
        queue="default",
        block=True
    )

    time.sleep(3)

    mem_stop = worker.get_report(with_memory=True)["process"]["mem"]["total"]

    diff = mem_stop - mem_start

    print "Memory diff for %s tasks was %s" % (n_tasks, diff)

    return diff


def test_memoryleaks_noleak(worker):

    TRACE = ""
    # TRACE = "--trace_memory_type ObjectId"

    worker.start(
        flags="--trace_memory --greenlets 1 --mongodb_logs 0 --report_interval 10000 %s" % TRACE)

    # Send it once to add to imports
    get_diff_after_jobs(worker, 10, 0)

    diff100 = get_diff_after_jobs(worker, 100, 0)

    assert worker.mongodb_jobs.mrq_jobs.count() == 100 + 10

    diff200 = get_diff_after_jobs(worker, 200, 0)

    assert worker.mongodb_jobs.mrq_jobs.count() == 200 + 100 + 10

    # Most of the tasks should have mem_diff == 0
    assert worker.mongodb_jobs.mrq_jobs.find(
        {"memory_diff": 0}).count() > 310 * 0.95

    assert diff100 < 15000
    assert diff200 < 15000

    assert worker.mongodb_jobs.mrq_workers.find().count() == 1


def test_memoryleaks_1mleak(worker):

    worker.start(
        flags="--trace_memory --greenlets 1 --mongodb_logs 0 --report_interval 10000")

    # Send it once to add to imports
    get_diff_after_jobs(worker, 10, 0)

    worker.mongodb_jobs.mrq_jobs.remove()

    # 1M leak!
    # sleep is needed so that psutil measurements are accurate :-/
    diff1m = get_diff_after_jobs(worker, 10, 100000, sleep=0)

    assert diff1m > 900000

    assert worker.mongodb_jobs.mrq_jobs.find(
        {"memory_diff": {"$gte": 80000}}).count() == 10

    assert worker.mongodb_jobs.mrq_workers.find().count() == 1
