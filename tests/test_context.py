import json
import time
import os
import pytest


def test_context_get(worker):

    result = worker.send_task("tests.tasks.context.GetContext", {})

    assert result["job_id"]
    assert result["worker_id"]
    assert result["config"]["redis"]


def test_context_connections_redis(worker):

    worker.start(flags=" --config tests/fixtures/config-multiredis.py")

    assert worker.send_task("tests.tasks.redis.MultiRedis", {}) == "ok"


def test_context_metric_success(worker):

    worker.start(flags=" --config tests/fixtures/config-metric.py")

    result = worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1})
    result = worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1})

    assert result == 42

    metrics = json.loads(
        worker.send_task("tests.tasks.general.GetMetrics", {}))

    # GetMetrics is also a task!
    assert metrics.get("queues.default.dequeued") == 3
    assert metrics.get("queues.all.dequeued") == 3

    # Queued from the test process, not the worker one...
    # assert metrics.get("queues.default.enqueued") == 3
    # assert metrics.get("queues.all.enqueued") == 3
    # assert metrics.get("jobs.status.queued") == 3

    assert metrics.get("jobs.status.started") == 3
    assert metrics.get("jobs.status.success") == 2


def test_context_metric_queue(worker):

    worker.start(flags=" --config tests/fixtures/config-metric.py")

    worker.send_task("tests.tasks.general.SendTask", {
                     "path": "tests.tasks.general.Add", "params": {"a": 41, "b": 1}})

    metrics = json.loads(
        worker.send_task("tests.tasks.general.GetMetrics", {}))

    # GetMetrics is also a task!
    assert metrics.get("queues.default.dequeued") == 3
    assert metrics.get("queues.all.dequeued") == 3
    assert metrics.get("jobs.status.started") == 3
    assert metrics.get("jobs.status.success") == 2

    assert metrics.get("queues.default.enqueued") == 1
    assert metrics.get("queues.all.enqueued") == 1
    assert metrics.get("jobs.status.queued") == 1


def test_context_metric_failed(worker):

    worker.start(flags=" --config tests/fixtures/config-metric.py")

    worker.send_task(
        "tests.tasks.general.RaiseException", {}, accept_statuses=["failed"])

    metrics = json.loads(
        worker.send_task("tests.tasks.general.GetMetrics", {}))

    # GetMetrics is also a task!
    assert metrics.get("queues.default.dequeued") == 2
    assert metrics.get("queues.all.dequeued") == 2
    assert metrics.get("jobs.status.started") == 2
    assert metrics.get("jobs.status.failed") == 1
    assert metrics.get("jobs.status.success") is None


def test_context_setup():

    try:
        import subprocess32 as subprocess
    except:
        import subprocess

    process = subprocess.Popen("python tests/fixtures/standalone_script1.py",
                               shell=True, close_fds=True, env={"MRQ_NAME": "testname1", "PYTHONPATH": os.getcwd()}, cwd=os.getcwd(), stdout=subprocess.PIPE)

    out, err = process.communicate()

    assert out.endswith("42\ntestname1\n")


@pytest.mark.parametrize(["gevent_count", "subpool_size", "iterations", "expected_clients"], [
    (None, None, 1, 1),     # single task opens a single connection
    (None, None, 2, 1),
    (None, 10, 1, 10),      # single task with subpool of 10 opens 10 connections
    (None, 10, 2, 10),
    (None, 200, 1, 100),    # single task with subpool of 200 opens 100 connections, we reach the max_connections limit
    (None, 200, 2, 100),
    (4, None, 1, 4),        # 4 gevent workers with a single task each : 4 connections
    (4, None, 2, 4),
    (2, 2, 1, 4),           # 2 gevent workers with 2 single tasks each : 4 connections
    (2, 2, 2, 4),

])
def test_redis_disconnections(gevent_count, subpool_size, iterations, expected_clients, worker):
    """ mrq.context.connections is not the actual connections pool that the worker uses.
        this worker's pool is not accessible from here, since it runs in a different thread.
    """
    from mrq.context import connections

    gevent_count = gevent_count if gevent_count is not None else 1

    get_clients = lambda: [c for c in connections.redis.client_list() if c.get("cmd") != "client"]
    # 1. start the worker and asserts that there is a redis client connected
    kwargs = {"flags": "--redis_max_connections 100"}
    if gevent_count:
        kwargs["flags"] += " --gevent %s" % gevent_count

    worker.start(**kwargs)

    for i in range(0, iterations):
        # sending tasks has the good side effect to wait for the worker to connect to redis
        worker.send_tasks("tests.tasks.redis.Disconnections", [{"subpool_size": subpool_size}] * gevent_count)

    assert len(get_clients()) == expected_clients

    # 2. kill the worker and make sure that the connection was closed
    worker.stop(deps=False)  # so that we still have access to redis
    assert len(get_clients()) == 0
