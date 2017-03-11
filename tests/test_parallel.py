from builtins import range
import time
import pytest
from mrq.context import connections


@pytest.mark.parametrize(["p_flags"], [
    ["--greenlets 50"],
    ["--processes 10 --greenlets 5"]
])
def test_parallel_100sleeps(worker, p_flags):

    worker.start(flags=p_flags)

    print("Worker started. Queueing sleeps")

    start_time = time.time()

    # This will sleep a total of 100 seconds
    result = worker.send_tasks(
        "tests.tasks.general.Add", [{"a": i, "b": 0, "sleep": 1} for i in range(100)])

    total_time = time.time() - start_time

    # But should be done quickly!
    assert total_time < 15

    # ... and return correct results
    assert result == list(range(100))


@pytest.mark.parametrize(["p_greenlets", "p_strategy"], [
    [g, s]
    for g in [1, 2]
    for s in ["", "parallel", "burst"]
])
def test_dequeue_strategy(worker, p_greenlets, p_strategy):

    worker.start_deps(flush=True)

    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 41, "sleep": 1}, queue="q1", block=False, start=False)
    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 42, "sleep": 1}, queue="q2", block=False, start=False)
    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 43, "sleep": 1}, queue="q1", block=False, start=False)
    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 44, "sleep": 1}, queue="q2", block=False, start=False)
    worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 45, "sleep": 1}, queue="q3", block=False, start=False)

    time.sleep(0.5)

    flags = "--greenlets %s" % p_greenlets
    if p_strategy:
        flags += " --dequeue_strategy %s" % p_strategy

    print("Worker has flags %s" % flags)
    worker.start(flags=flags, queues="q1 q2", deps=False, block=False)

    gotit = worker.wait_for_idle()

    if p_strategy == "burst":
        assert not gotit  # because worker should be stopped already
    else:
        assert gotit

    inserts = list(connections.mongodb_jobs.tests_inserts.find(sort=[("_id", 1)]))
    order = [row["params"]["a"] for row in inserts]

    if p_strategy == "parallel":
        assert set(order[0:2]) == set([41, 42])
        assert set(order[2:4]) == set([43, 44])
    else:
        assert set(order[0:2]) == set([41, 43])
        assert set(order[2:4]) == set([42, 44])
