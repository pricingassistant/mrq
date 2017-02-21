import json
import os
from collections import defaultdict

# Read from tests.tasks.general.GetMetrics
TEST_LOCAL_METRICS = defaultdict(int)


def METRIC_HOOK(name, incr=1, **kwargs):
    TEST_LOCAL_METRICS[name] += incr


def _reset_local_metrics():
    for k in TEST_LOCAL_METRICS.keys():
        TEST_LOCAL_METRICS.pop(k)


def test_context_get(worker):

    result = worker.send_task("tests.tasks.context.GetContext", {})

    assert result["job_id"]
    assert result["worker_id"]
    assert result["config"]["redis"]


def test_context_connections_redis(worker):

    worker.start(flags=" --config tests/fixtures/config-multiredis.py")

    assert worker.send_task("tests.tasks.redis.MultiRedis", {}) == "ok"


def test_context_metric_success(worker):
    from mrq.context import get_current_config

    local_config = get_current_config()
    local_config["metric_hook"] = METRIC_HOOK
    _reset_local_metrics()

    worker.start(flags=" --config tests/fixtures/config-metric.py")

    result = worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1})
    result = worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1})

    assert result == 42

    metrics = json.loads(
        worker.send_task("tests.tasks.general.GetMetrics", {}))

    # GetMetrics is also a task!
    assert metrics.get("queues.default.dequeued") == 3
    assert metrics.get("queues.all.dequeued") == 3

    TEST_LOCAL_METRICS.get("jobs.status.queued") == 3
    assert metrics.get("jobs.status.started") == 3
    assert metrics.get("jobs.status.success") == 2  # At the time it is run, GetMetrics isn't success yet.

    local_config["metric_hook"] = None


def test_context_metric_queue(worker):
    from mrq.context import get_current_config

    local_config = get_current_config()
    local_config["metric_hook"] = METRIC_HOOK
    _reset_local_metrics()

    worker.start(flags=" --config tests/fixtures/config-metric.py")

    # Will send 1 task inside!
    worker.send_task("tests.tasks.general.SendTask", {
                     "path": "tests.tasks.general.Add", "params": {"a": 41, "b": 1}})

    metrics = json.loads(
        worker.send_task("tests.tasks.general.GetMetrics", {}))

    # GetMetrics is also a task!
    assert metrics.get("queues.default.dequeued") == 3
    assert metrics.get("queues.all.dequeued") == 3
    assert metrics.get("jobs.status.started") == 3
    assert metrics.get("jobs.status.success") == 2  # At the time it is run, GetMetrics isn't success yet.

    TEST_LOCAL_METRICS.get("queues.default.enqueued") == 2
    TEST_LOCAL_METRICS.get("queues.all.enqueued") == 2

    local_config["metric_hook"] = None


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

    assert out.endswith(b"42\ntestname1\n")
