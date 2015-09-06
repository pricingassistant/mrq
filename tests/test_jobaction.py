from mrq.job import Job
from mrq.queue import Queue
import time
import pytest


OPTS = []
for p_query in [
    # Query, number_matching
    ({"path": "tests.tasks.general.MongoInsert"}, 3),
    ({"queue": "q1"}, 2),
    ({"params": "{\"a\": 44}"}, 1)
]:
    OPTS.append([p_query])


@pytest.mark.parametrize(["p_query"], OPTS)
def test_cancel_by_path(worker, p_query):

    expected_action_jobs = p_query[1]

    # Start the worker with only one greenlet so that tasks execute
    # sequentially
    worker.start(flags="--greenlets 1", queues="default q1 q2")

    job_ids = []
    job_ids.append(worker.send_task("tests.tasks.general.Add", {
                   "a": 41, "b": 1, "sleep": 2}, queue="default", block=False))

    params = {
        "action": "cancel",
        "status": "queued"
    }
    params.update(p_query[0])

    requeue_job = worker.send_task(
        "mrq.basetasks.utils.JobAction", params, block=False)

    job_ids.append(worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 42}, queue="q1", block=False))
    job_ids.append(worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 42}, queue="q2", block=False))
    job_ids.append(worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 43}, queue="q2", block=False))
    job_ids.append(worker.send_task(
        "tests.tasks.general.MongoInsert2", {"a": 44}, queue="q1", block=False))

    Job(job_ids[-1]).wait(poll_interval=0.01)

    # Leave some time to unqueue job_id4 without executing.
    time.sleep(1)
    worker.stop(deps=False)

    jobs = [Job(job_id).fetch().data for job_id in job_ids]

    assert jobs[0]["status"] == "success"
    assert jobs[0]["result"] == 42

    assert Job(requeue_job).fetch().data["result"][
        "cancelled"] == expected_action_jobs

    # Check that the right number of jobs ran.
    assert worker.mongodb_jobs.tests_inserts.count() == len(
        job_ids) - 1 - expected_action_jobs

    action_jobs = list(worker.mongodb_jobs.mrq_jobs.find({"status": "cancel"}))
    assert len(action_jobs) == expected_action_jobs
    assert set([x.get("result") for x in action_jobs]) == set([None])

    assert Queue("default").size() == 0
    assert Queue("q1").size() == 0
    assert Queue("q2").size() == 0

    worker.mongodb_jobs.tests_inserts.remove({})

    # Then requeue the same jobs
    params = {
        "action": "requeue"
    }
    params.update(p_query[0])

    worker.start(flags="--gevent 1", start_deps=False, queues="default", flush=False)

    ret = worker.send_task("mrq.basetasks.utils.JobAction", params, block=True)

    assert ret["requeued"] == expected_action_jobs

    worker.stop(deps=False)

    assert worker.mongodb_jobs.mrq_jobs.find(
        {"status": "queued"}).count() == expected_action_jobs

    assert Queue("default").size() + Queue("q1").size() + \
        Queue("q2").size() == expected_action_jobs

    worker.stop_deps()
