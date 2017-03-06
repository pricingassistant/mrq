from builtins import range
import time
import pytest
from mrq.context import connections
from mrq.job import Job


def test_queue_notify(worker, worker2):

    worker.start(flags="--max_latency 30 --config tests/fixtures/config-notify.py", queues="withnotify withoutnotify", bind_admin_port=False)

    # Used to queue jobs in the same environment & config!
    worker2.start(flags="--config tests/fixtures/config-notify.py")

    time.sleep(4)

    id1 = worker2.send_task("tests.tasks.general.SendTask", {
        "params": {"a": 42, "b": 1},
        "path": "tests.tasks.general.Add",
        "queue": "withnotify"
    })

    time.sleep(2)

    assert Job(id1).fetch().data["status"] == "success"
    assert Job(id1).fetch().data["result"] == 43

    id2 = worker2.send_task("tests.tasks.general.SendTask", {
        "params": {"a": 43, "b": 1},
        "path": "tests.tasks.general.Add",
        "queue": "withoutnotify"
    })

    time.sleep(2)

    assert Job(id2).fetch().data["status"] == "queued"
