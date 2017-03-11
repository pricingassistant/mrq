import time
import pytest
from mrq.job import Job
from mrq.queue import Queue


@pytest.mark.parametrize(["queues", "enqueue_on"], [
    [["main/", "second/"], ["main/", "main/sub", "main/sub/nested", "second/x"]],
    [["prefix/main/"], ["prefix/main/", "prefix/main/sub", "prefix/main/sub/nested"]],
])
def test_matchable_subqueues(worker, queues, enqueue_on):
    worker.start(queues=" ".join(queues), flags="--subqueues_refresh_interval=0.1")

    job_ids = []

    for subqueue in enqueue_on:
        job_id = worker.send_task("tests.tasks.general.GetTime", {}, queue=subqueue, block=False)
        job_ids.append(job_id)

    assert all([Job(j).wait(poll_interval=0.01, timeout=3) for j in job_ids])
    worker.stop()


@pytest.mark.parametrize(["queue", "enqueue_on"], [
    ["main/", ["/main", "main_", "/", "main", "other"]],
    ["prefix/main/", ["prefix", "prefix/other", "prefix/main"]],
])
def test_unmatchable_subqueues(worker, queue, enqueue_on):
    worker.start(queues=queue, flags="--subqueues_refresh_interval=0.1")

    job_ids = []

    for subqueue in enqueue_on:
        job_id = worker.send_task("tests.tasks.general.GetTime", {}, queue=subqueue, block=False)
        job_ids.append(job_id)

    time.sleep(2)
    results = [Job(j).fetch().data.get("status") for j in job_ids]

    # ensure tasks are not consumed by a worker
    assert results == ["queued"] * len(results)

    worker.stop()


def test_refresh_interval(worker):

    """ Tests that a refresh interval of 0 disables the subqueue detection """

    worker.start(queues="test/", flags="--subqueues_refresh_interval=0")

    time.sleep(2)

    job_id1 = worker.send_task(
        "tests.tasks.general.GetTime", {"a": 41},
        queue="test/subqueue", block=False)

    time.sleep(5)

    job1 = Job(job_id1).fetch().data

    assert job1["status"] == "queued"

    worker.stop()
