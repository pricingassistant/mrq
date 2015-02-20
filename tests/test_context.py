import json
import os


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

