import time
from mrq.queue import Queue
import pytest

@pytest.mark.parametrize(["p_max_latency", "p_min_observed_latency", "p_max_observed_latency"], [
    [1, 0.03, 1],
    [0.01, -1, 0.02]
])
def test_job_max_latency(worker, p_max_latency, p_min_observed_latency, p_max_observed_latency):

    worker.start(flags=" --greenlets=1 --max_latency=%s" % (p_max_latency), trace=False)

    def get_latency():
        t = time.time()
        return worker.send_task("tests.tasks.general.GetTime", {}) - t

    # Warm up the worker
    get_latency()

    # This is the latency induced by our test system & general task work
    # We're on the same machine so even in different processes time.time() should be pretty reliable
    base_latency = get_latency()
    print "Base latency: %ss" % base_latency

    min_latency = min([get_latency() for _ in range(0, 5)])
    print "FYI, min latency = %ss" % min_latency

    # Sleep a while with an idle worker to make the poll interval go up
    time.sleep(30)

    # TODO, will fail if sleeps synchronize and by chance we send the task just when a sleep() is finished
    # Average several ones over an interval instead?
    latency = get_latency() - base_latency

    print "Observed latency: %ss" % latency

    assert p_min_observed_latency <= latency < p_max_observed_latency


@pytest.mark.parametrize(["p_latency", "p_min", "p_max"], [
    [0, 0, 3],
    ["0.05", 4, 20],
    ["0.05-0.1", 4, 20]
])
def test_network_latency(worker, p_latency, p_min, p_max):

    worker.start(flags=" --no_mongodb_ensure_indexes --add_network_latency=%s" % (p_latency), trace=False)

    start_time = time.time()

    for _ in range(5):
        worker.send_task("tests.tasks.general.MongoInsert", {"x": 1})

    total_time = time.time() - start_time

    assert p_min < total_time < p_max


def benchmark_task(worker, taskpath, taskparams, tasks=1000, greenlets=50, processes=0, max_seconds=10, profile=False, quiet=True, raw=False, queues="default", config=None):

    worker.start(flags="--processes %s --greenlets %s%s%s%s" % (
        processes,
        greenlets,
        " --profile" if profile else "",
        " --quiet" if quiet else "",
        " --config %s" % config if config else ""
    ), queues=queues, trace=False)

    # Warm up the workers with one simple task.
    print "Warming up workers..."
    worker.send_tasks("tests.tasks.general.Add", [{"a": i, "b": 0, "sleep": 0} for i in range(greenlets * min(1, processes))])

    print "Starting benchmark..."
    start_time = time.time()

    # result = worker.send_tasks("tests.tasks.general.Add",
    #                            [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)])

    if raw:
        result = worker.send_raw_tasks(taskpath, taskparams)
    else:
        result = worker.send_tasks(taskpath, taskparams)

    total_time = time.time() - start_time

    print "%s tasks done with %s greenlets and %s processes in %0.3f seconds : %0.2f jobs/second!" % (tasks, greenlets, processes, total_time, tasks / total_time)

    assert total_time < max_seconds

    return result, total_time


@pytest.mark.parametrize(["p_processes"], [[0], [2]])
def test_performance_simpleadds_regular(worker, p_processes):

    n_tasks = 10000
    n_greenlets = 30
    n_processes = p_processes
    max_seconds = 35

    result, total_time = benchmark_task(worker,
                                        "tests.tasks.general.Add",
                                        [{"a": i, "b": 0, "sleep": 0}
                                            for i in range(n_tasks)],
                                        tasks=n_tasks,
                                        greenlets=n_greenlets,
                                        processes=n_processes,
                                        max_seconds=max_seconds)

    # ... and return correct results
    assert result == range(n_tasks)


@pytest.mark.parametrize(["p_queue", "p_greenlets"], [x1 + x2 for x1 in [
    ["testperformance_raw"],
    ["testperformance_set"],
    ["testperformance_timed_set"]
] for x2 in [
    [100]
]])
def test_performance_simpleadds_raw(worker, p_queue, p_greenlets):

    n_tasks = 10000
    n_greenlets = p_greenlets
    n_processes = 0
    max_seconds = 35

    result, total_time = benchmark_task(worker,
                                        p_queue,
                                        [str(i) for i in range(n_tasks)],
                                        tasks=n_tasks,
                                        greenlets=n_greenlets,
                                        processes=n_processes,
                                        max_seconds=max_seconds,
                                        raw=True,
                                        queues=p_queue,
                                        config="tests/fixtures/config-raw1.py")


# TODO add network latency
def test_performance_httpstatic_fast(worker, httpstatic):

    httpstatic.start()

    n_tasks = 1000
    n_greenlets = 50
    max_seconds = 10

    result, total_time = benchmark_task(worker,
                                        "tests.tasks.general.Fetch",
                                        [{"url": "http://127.0.0.1:8081/"}
                                            for _ in range(n_tasks)],
                                        tasks=n_tasks,
                                        greenlets=n_greenlets,
                                        max_seconds=max_seconds,
                                        profile=False)


# def test_performance_httpstatic_external(worker):

#   n_tasks = 1000
#   n_greenlets = 100
#   max_seconds = 25

#   url = "http://bing.com/favicon.ico"
# url = "http://ox-mockserver.herokuapp.com/ipheaders"
# url = "http://ox-mockserver.herokuapp.com/timeout?timeout=1000"

#   result, total_time = benchmark_task(worker,
#                                       "tests.tasks.general.Fetch",
#                                       [{"url": url} for _ in range(n_tasks)],
#                                       tasks=n_tasks,
#                                       greenlets=n_greenlets,
#                                       max_seconds=max_seconds, quiet=False)


def test_performance_queue_cancel_requeue(worker):

    worker.start(trace=False)

    n_tasks = 10000

    start_time = time.time()

    worker.send_tasks(
        "tests.tasks.general.Add",
        [{"a": i, "b": 0, "sleep": 0} for i in range(n_tasks)],
        queue="noexec",
        block=False
    )

    queue_time = time.time() - start_time

    print "Queued %s tasks in %s seconds (%s/s)" % (n_tasks, queue_time, float(n_tasks) / queue_time)
    assert queue_time < 2

    assert Queue("noexec").size() == n_tasks
    assert worker.mongodb_jobs.mrq_jobs.count() == n_tasks
    assert worker.mongodb_jobs.mrq_jobs.find(
        {"status": "queued"}).count() == n_tasks

    # Then cancel them all
    start_time = time.time()

    res = worker.send_task(
        "mrq.basetasks.utils.JobAction",
        {"queue": "noexec", "action": "cancel"},
        block=True
    )
    assert res["cancelled"] == n_tasks
    queue_time = time.time() - start_time
    print "Cancelled %s tasks in %s seconds (%s/s)" % (n_tasks, queue_time, float(n_tasks) / queue_time)
    assert queue_time < 5
    assert worker.mongodb_jobs.mrq_jobs.find(
        {"status": "cancel"}).count() == n_tasks

    # Special case because we cancelled by queue: they should have been
    # removed from redis.
    assert Queue("noexec").size() == 0

    # Then requeue them all
    start_time = time.time()

    res = worker.send_task(
        "mrq.basetasks.utils.JobAction",
        {"queue": "noexec", "action": "requeue"},
        block=True
    )

    queue_time = time.time() - start_time
    print "Requeued %s tasks in %s seconds (%s/s)" % (n_tasks, queue_time, float(n_tasks) / queue_time)
    assert queue_time < 2
    assert worker.mongodb_jobs.mrq_jobs.find(
        {"status": "queued"}).count() == n_tasks

    # They should be back in the queue
    assert Queue("noexec").size() == n_tasks

    assert res["requeued"] == n_tasks
