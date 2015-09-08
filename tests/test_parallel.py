import time
import pytest
from mrq.context import connections


@pytest.mark.parametrize(["p_flags"], [
    ["--greenlets 50"],
    ["--processes 10 --greenlets 5"]
])
def test_parallel_100sleeps(worker, p_flags):

    worker.start(flags=p_flags)

    start_time = time.time()

    # This will sleep a total of 100 seconds
    result = worker.send_tasks(
        "tests.tasks.general.Add", [{"a": i, "b": 0, "sleep": 1} for i in range(100)])

    total_time = time.time() - start_time

    # But should be done quickly!
    assert total_time < 15

    # ... and return correct results
    assert result == range(100)


def test_dequeue_strategy(worker):

    worker.start_deps()

    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 41, "sleep": 2}, queue="q1", block=False)
    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 42, "sleep": 2}, queue="q2", block=False)
    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 41, "sleep": 2}, queue="q1", block=False)
    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 42, "sleep": 2}, queue="q2", block=False)

    time.sleep(0.1)

    worker.start(flags="--dequeue_strategy parallel --greenlets 2", queues="q1 q2", deps=False)

    time.sleep(1)

    # Should be dequeued in parallel
    assert connections.mongodb_jobs.tests_inserts.count({"params.a": 41}) == 1
    assert connections.mongodb_jobs.tests_inserts.count({"params.a": 42}) == 1
    assert connections.mongodb_jobs.tests_inserts.count() == 2
