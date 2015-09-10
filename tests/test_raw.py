from mrq.job import Job
import datetime
from mrq.queue import Queue
import time
import pytest


@pytest.mark.parametrize(["p_queue", "p_pushback", "p_timed", "p_flags"], [
    ["test_timed_set", False, True, "--greenlets 10"],
    ["pushback_timed_set", True, True, "--greenlets 10"],
    ["test_sorted_set", False, False, "--greenlets 1"]
])
def test_raw_sorted(worker, p_queue, p_pushback, p_timed, p_flags):

    worker.start(flags="%s --config tests/fixtures/config-raw1.py" %
                 p_flags, queues=p_queue)

    test_collection = worker.mongodb_logs.tests_inserts
    jobs_collection = worker.mongodb_jobs.mrq_jobs

    current_time = int(time.time())

    assert jobs_collection.count() == 0

    assert Queue(p_queue).size() == 0

    # Schedule one in the past, one in the future
    worker.send_raw_tasks(p_queue, {
        "aaa": current_time - 10,
        "bbb": current_time + 2,
        "ccc": current_time + 5
    }, block=False)

    # Re-schedule
    worker.send_raw_tasks(p_queue, {
        "ccc": current_time + 2
    }, block=False)

    time.sleep(1)

    if not p_timed:

        assert Queue(p_queue).size() == 0
        assert test_collection.count() == 3
        assert list(test_collection.find(projection={"params": 1, "_id": 0}).limit(1)) == [
            {"params": {"sorted_set": "aaa"}}
        ]
        return

    if p_pushback:
        assert Queue(p_queue).size() == 3
        assert set(Queue(p_queue).list_raw_jobs()) == set(["bbb", "ccc", "aaa"])
    else:
        assert Queue(p_queue).size() == 2
        assert set(Queue(p_queue).list_raw_jobs()) == set(["bbb", "ccc"])

    # The second one should not yet even exist in mrq_jobs
    assert jobs_collection.count() == 1
    assert list(jobs_collection.find())[0]["status"] == "success"

    assert list(test_collection.find(projection={"params": 1, "_id": 0})) == [
        {"params": {"timed_set": "aaa"}}
    ]

    # Then wait for the second job to be done
    time.sleep(2)

    if p_pushback:
        assert Queue(p_queue).size() == 3
    else:
        assert Queue(p_queue).size() == 0

    assert jobs_collection.count() == 3
    assert list(jobs_collection.find())[1]["status"] == "success"
    assert list(jobs_collection.find())[2]["status"] == "success"

    assert list(jobs_collection.find())[2]["worker"]

    assert test_collection.count() == 3


@pytest.mark.parametrize(["p_queue", "p_set"], [
    ["test_raw", False],
    ["test_set", True]
])
def test_raw_set(worker, p_queue, p_set):

    worker.start(
        flags="--greenlets 10 --config tests/fixtures/config-raw1.py", queues=p_queue)

    test_collection = worker.mongodb_logs.tests_inserts
    jobs_collection = worker.mongodb_jobs.mrq_jobs

    assert jobs_collection.count() == 0

    assert Queue(p_queue).size() == 0

    # Schedule one in the past, one in the future
    worker.send_raw_tasks(p_queue, ["aaa", "bbb", "ccc", "bbb"], block=True)

    if p_set:
        assert test_collection.count() == 3

    else:
        assert test_collection.count() == 4


def test_raw_started(worker):

    worker.start(
        flags="--greenlets 2 --config tests/fixtures/config-raw1.py", queues="teststarted_raw teststartedx")

    worker.send_raw_tasks("teststarted_raw", ["f1", "f2", "f3"], block=False)
    time.sleep(2)
    jobs_collection = worker.mongodb_jobs.mrq_jobs

    assert jobs_collection.find({"status": "started", "queue": "teststartedx"}).count() == 2
    assert jobs_collection.count() == 2

    worker.mongodb_jobs.tests_flags.insert({"flag": "f1"})
    time.sleep(1)

    assert jobs_collection.find({"status": "success", "queue": "teststartedx"}).count() == 1
    assert jobs_collection.find({"status": "started", "queue": "teststartedx"}).count() == 2
    assert jobs_collection.count() == 3

    worker.mongodb_jobs.tests_flags.insert({"flag": "f2"})
    worker.mongodb_jobs.tests_flags.insert({"flag": "f3"})
    time.sleep(1)

    worker.stop(block=True)

    assert jobs_collection.find({"status": "success", "queue": "teststartedx"}).count() == 3
    assert jobs_collection.count() == 3


@pytest.mark.parametrize(["p_queue"], [
    ["test_raw"],
    ["test_set"],
    ["test_timed_set"]

])
def test_raw_remove(worker, p_queue):

    worker.start_deps()

    worker.send_raw_tasks(
        p_queue, ["aa", "bb", "cc"], block=False, start=False)

    assert Queue(p_queue).size() == 3

    Queue(p_queue).remove_raw_jobs(["aa", "cc"])

    assert Queue(p_queue).size() == 1

    worker.stop_deps()


def test_raw_exception(worker):

    p_queue = "testexception_raw"

    worker.start(
        flags="--greenlets 10 --config tests/fixtures/config-raw1.py", queues=p_queue)

    jobs_collection = worker.mongodb_jobs.mrq_jobs
    assert jobs_collection.count() == 0
    assert Queue(p_queue).size() == 0

    worker.send_raw_tasks(p_queue, ["msg1"], block=True)

    failjob = list(jobs_collection.find())[0]

    assert Queue("default").size() == 0
    assert Queue(p_queue).size() == 0
    assert jobs_collection.count() == 1
    assert failjob["status"] == "failed"

    worker.stop(deps=False)

    worker.start(
        deps=False, flags="--greenlets 10 --config tests/fixtures/config-raw1.py", queues="default")

    worker.send_task(
        "mrq.basetasks.utils.JobAction",
        {
            "id": failjob["_id"],
            "action": "requeue"
        },
        block=True
    )

    assert Queue("default").size() == 0
    assert Queue(p_queue).size() == 0
    assert Queue("testx").size() == 1
    assert jobs_collection.count() == 2
    assert list(jobs_collection.find({"_id": failjob["_id"]}))[
        0]["status"] == "queued"
    assert list(jobs_collection.find({"_id": {"$ne": failjob["_id"]}}))[
        0]["status"] == "success"

    worker.stop(deps=False)

    worker.start(
        deps=False, flags="--greenlets 10 --config tests/fixtures/config-raw1.py", queues="default testx")

    time.sleep(2)

    assert Queue(p_queue).size() == 0
    assert jobs_collection.count() == 2
    assert Queue("testx").size() == 0
    assert list(jobs_collection.find({"_id": failjob["_id"]}))[
        0]["status"] == "failed"


def test_raw_retry(worker):

    p_queue = "testretry_raw"

    worker.start(
        flags="--greenlets 10 --config tests/fixtures/config-raw1.py", queues=p_queue)

    jobs_collection = worker.mongodb_jobs.mrq_jobs
    assert jobs_collection.count() == 0
    assert Queue(p_queue).size() == 0

    worker.send_raw_tasks(p_queue, [0], block=True)

    failjob = list(jobs_collection.find())[0]

    assert Queue("default").size() == 0
    assert Queue("testx").size() == 1

    assert Queue(p_queue).size() == 0
    assert jobs_collection.count() == 1
    assert failjob["status"] == "queued"
    assert failjob["queue"] == "testx"


@pytest.mark.parametrize(["p_queue", "p_greenlets"], [x1 + x2 for x1 in [
    ["test_raw default test"],
    # ["default test_raw test"],
    # ["default test_raw test_set"],
    # ["test_set test_raw default"],
    # ["test test2 test_set test_raw default"]
] for x2 in [
    #[1],
    [2],
    #[10]
]])
def test_raw_mixed(worker, p_queue, p_greenlets):

    worker.start_deps()

    worker.send_raw_tasks(
        "test_raw", ["aaa", "bbb", "ccc"], start=False, block=False)

    worker.send_task("tests.tasks.general.MongoInsert", {
        "not_raw": "ddd"
    }, start=False, block=False)

    assert Queue("test_raw").size() == 3
    assert Queue("default").size() == 1

    worker.start(flags="--greenlets %s --config tests/fixtures/config-raw1.py" %
                 p_greenlets, queues=p_queue, deps=False)

    test_collection = worker.mongodb_logs.tests_inserts
    jobs_collection = worker.mongodb_jobs.mrq_jobs

    time.sleep(1)

    assert Queue("test_raw").size() == 0
    assert Queue("default").size() == 0

    assert test_collection.count() == 4
    assert jobs_collection.count() == 4
    assert jobs_collection.find({"status": "success"}).count() == 4

    assert list(jobs_collection.find({"status": "success"}))[0]["worker"]


def test_raw_no_storage(worker):
    """ Test tasks that don't store unless they go to error status like 'failed' """

    worker.start(
        flags="--config tests/fixtures/config-raw1.py",
        queues="default testnostorage_raw"
    )

    jobs_collection = worker.mongodb_jobs.mrq_jobs
    test_collection = worker.mongodb_logs.tests_inserts

    worker.send_raw_tasks("testnostorage_raw", [
        "tests.tasks.general.MongoInsert 3"
    ], block=False)

    time.sleep(2)

    # No started inserted.
    assert jobs_collection.count() == 0

    time.sleep(2)

    # No success either, but we did insert
    assert test_collection.count() == 1
    assert jobs_collection.count() == 0
    test_collection.remove({})

    # However failed tasks get stored.

    worker.send_raw_tasks("testnostorage_raw", [
        "tests.tasks.general.RaiseException 0"
    ], block=False)

    time.sleep(2)

    # Failed was inserted.
    assert jobs_collection.count({"status": "failed", "path": "tests.tasks.general.RaiseException"}) == 1

    # If we requeue and don't raise, should be OK and inserted this time, even in success
    # no_storage depends on a raw queue, not a task path.
    _id = jobs_collection.find_one()["_id"]
    jobs_collection.update({"_id": _id}, {"$set": {"path": "tests.tasks.general.MongoInsert"}})
    job = Job(_id).fetch(full_data=True)
    job.requeue(queue="default")

    time.sleep(1)
    assert test_collection.count() == 1
    assert jobs_collection.count() == 1
    assert jobs_collection.count({"status": "success"}) == 1

    jobs_collection.remove({})

    # Test with retry: should be inserted
    worker.send_raw_tasks("testnostorage_raw", [
        "tests.tasks.general.Retry 0"
    ], block=False)

    assert jobs_collection.count({"status": "started"}) == 0

    time.sleep(2)

    assert jobs_collection.count({"status": "retry"}) == 1
