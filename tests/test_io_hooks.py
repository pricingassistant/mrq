import json


def test_io_hooks_nothing(worker):

    worker.start(flags=" --config tests/fixtures/config-io-hooks.py")

    assert worker.send_task(
        "tests.tasks.general.Add", {"a": 41, "b": 1}) == 42

    events = json.loads(
        worker.send_task("tests.tasks.general.GetIoHookEvents", {}))

    job_events = [x for x in events if x.get("job")]

    for evt in job_events:
        print evt

    # Only update should be the result in mongodb.
    assert len(job_events) == 1 * 2

    assert job_events[0]["hook"] == "mongodb_pre"
    assert job_events[1]["hook"] == "mongodb_post"

    assert job_events[0]["method"] == "update"
    assert job_events[1]["method"] == "update"

    assert job_events[0]["collection"] == "mrq.mrq_jobs"
    assert job_events[1]["collection"] == "mrq.mrq_jobs"


def test_io_hooks_redis(worker):

    worker.start(flags=" --config tests/fixtures/config-io-hooks.py")

    worker.send_task(
        "tests.tasks.io.TestIo",
        {"test": "redis-llen", "params": {"key": "xyz"}}
    )

    events = json.loads(
        worker.send_task("tests.tasks.general.GetIoHookEvents", {}))

    job_events = [x for x in events if x.get("job")]

    for evt in job_events:
        print evt

    assert len(job_events) == 2 * 2

    assert job_events[0]["hook"] == "redis_pre"
    assert job_events[1]["hook"] == "redis_post"

    assert job_events[0]["key"] == "xyz"
    assert job_events[1]["key"] == "xyz"

    assert job_events[0]["command"] == "LLEN"
    assert job_events[1]["command"] == "LLEN"

    # Regular MongoDB update

    assert job_events[2]["hook"] == "mongodb_pre"
    assert job_events[3]["hook"] == "mongodb_post"

    assert job_events[2]["method"] == "update"
    assert job_events[3]["method"] == "update"

    assert job_events[2]["collection"] == "mrq.mrq_jobs"
    assert job_events[3]["collection"] == "mrq.mrq_jobs"


def test_io_hooks_mongodb(worker):

    worker.start(flags=" --config tests/fixtures/config-io-hooks.py")

    worker.send_task(
        "tests.tasks.io.TestIo",
        {"test": "mongodb-full-getmore"}
    )

    events = json.loads(
        worker.send_task("tests.tasks.general.GetIoHookEvents", {}))

    job_events = [x for x in events if x.get("job")]

    for evt in job_events:
        print evt

    assert len(job_events) == 4 * 2

    # First, insert
    assert job_events[0]["hook"] == "mongodb_pre"
    assert job_events[1]["hook"] == "mongodb_post"

    assert job_events[0]["collection"] == "mrq.tests_inserts"
    assert job_events[1]["collection"] == "mrq.tests_inserts"

    assert job_events[0]["method"] == "insert_many"
    assert job_events[1]["method"] == "insert_many"

    # Then first query
    assert job_events[2]["hook"] == "mongodb_pre"
    assert job_events[3]["hook"] == "mongodb_post"

    assert job_events[2]["collection"] == "mrq.tests_inserts"
    assert job_events[3]["collection"] == "mrq.tests_inserts"

    assert job_events[2]["method"] == "find"
    assert job_events[3]["method"] == "find"

    # Then getmore query
    assert job_events[4]["hook"] == "mongodb_pre"
    assert job_events[5]["hook"] == "mongodb_post"

    assert job_events[4]["collection"] == "mrq.tests_inserts"
    assert job_events[5]["collection"] == "mrq.tests_inserts"

    assert job_events[4]["method"] == "cursor"
    assert job_events[5]["method"] == "cursor"

    # Result MongoDB update

    assert job_events[6]["hook"] == "mongodb_pre"
    assert job_events[7]["hook"] == "mongodb_post"

    assert job_events[6]["method"] == "update"
    assert job_events[7]["method"] == "update"

    assert job_events[6]["collection"] == "mrq.mrq_jobs"
    assert job_events[7]["collection"] == "mrq.mrq_jobs"
