import time
import pytest
from mrq.job import Job


@pytest.mark.parametrize(["queue", "enqueue_on"], [
    ["main/", ["main", "main/", "main/sub", "main/sub/nested"]],
    ["prefix/main/", ["prefix/main", "prefix/main/", "prefix/main/sub", "prefix/main/sub/nested"]],
])
def test_matchable_subqueues(worker, queue, enqueue_on):
    worker.start(queues=queue)

    job_ids = []

    for subqueue in enqueue_on:
        job_id = worker.send_task("tests.tasks.general.GetTime", {}, queue=subqueue, block=False)
        job_ids.append(job_id)

    assert map(lambda j: Job(j).wait(poll_interval=0.01, timeout=3), job_ids)
    worker.stop()


@pytest.mark.parametrize(["queue", "enqueue_on"], [
    ["main/", ["/main", "main_", "/"]],
    ["prefix/main/", ["prefix", "prefix/other"]],
])
def test_unmatchable_subqueues(worker, queue, enqueue_on):
    worker.start(queues=queue)

    job_ids = []

    for subqueue in enqueue_on:
        job_id = worker.send_task("tests.tasks.general.GetTime", {}, queue=subqueue, block=False)
        job_ids.append(job_id)

    time.sleep(2)
    results = map(lambda j: Job(j).fetch().data.get("status"), job_ids)

    # ensure tasks are not consumed by a worker
    assert results[0] == "queued"
    assert all(results) == True

    worker.stop()


@pytest.mark.parametrize(["delimiter"], ["/", ".", "_"])
def test_custom_delimiters(worker, delimiter):

    queue = "main" + delimiter
    subqueue = queue + "subqueue"

    worker.start(queues=queue, flags=" --subqueues_delimiter=%s" % delimiter)
    job_id = worker.send_task("tests.tasks.general.GetTime", {}, queue=subqueue, block=False)
    Job(job_id).wait(poll_interval=0.01)
    print vars(Job(job_id))
    worker.stop()
