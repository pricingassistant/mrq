import time
import pytest
import datetime


# We want to test that launching the scheduler several times queues tasks
# only once.
PROCESS_CONFIGS = [
    ["--greenlets 1"],
    ["--greenlets 1 --processes 5"]
]


@pytest.mark.parametrize(["p_flags"], PROCESS_CONFIGS)
def test_scheduler_simple(worker, p_flags):

    worker.start(
        flags="--scheduler --config tests/fixtures/config-scheduler1.py %s" % p_flags)

    collection = worker.mongodb_jobs.tests_inserts
    scheduled_jobs = worker.mongodb_jobs.mrq_scheduled_jobs

    while not collection.count():
        time.sleep(1)

    # There are 4 test tasks with 5 second interval
    inserts = list(collection.find())
    assert len(inserts) == 4

    jobs = list(scheduled_jobs.find())
    assert len(jobs) == 4

    time.sleep(5)

    # They should have ran again.
    inserts = list(collection.find())
    assert len(inserts) == 8

    worker.stop(deps=False)

    collection.remove({})

    # Start with new config
    worker.start(
        deps=False, flags="--scheduler --config tests/fixtures/config-scheduler2.py %s" % p_flags)

    while not collection.count():
        time.sleep(1)

    jobs2 = list(scheduled_jobs.find())
    assert len(jobs2) == 4
    assert jobs != jobs2

    # Only 3 should have been replaced and ran immediately again because they
    # have different config.
    inserts = list(collection.find())
    print inserts
    assert len(inserts) == 3, inserts


@pytest.mark.parametrize(["p_flags"], PROCESS_CONFIGS)
def test_scheduler_dailytime(worker, p_flags):

    # Task is scheduled in 3 seconds
    worker.start(
        flags="--scheduler --config tests/fixtures/config-scheduler3.py %s" % p_flags,
        env={
            # We need to pass this in the environment so that each worker has the
            # exact same hash
            "MRQ_TEST_SCHEDULER_TIME": str(time.time() + 5)
        })

    collection = worker.mongodb_jobs.tests_inserts
    assert collection.find().count() == 0

    # It should be done a first time immediately
    time.sleep(3)
    inserts = list(collection.find())
    assert len(inserts) == 2
    print inserts
    assert collection.find({"params.b": "test"}).count() == 1

    # Then a second time once the dailytime passes
    time.sleep(7)
    assert collection.find().count() == 4
    assert collection.find({"params.b": "test"}).count() == 2

    # Nothing more should happen today
    time.sleep(4)
    assert collection.find().count() == 4
    assert collection.find({"params.b": "test"}).count() == 2


def test_scheduler_weekday(worker):
    # Task is scheduled in 3 seconds
    worker.start(
        flags="--scheduler --config tests/fixtures/config-scheduler4.py",
        env={
            # We need to pass this in the environment so that each worker has the
            # exact same hash
            "MRQ_TEST_SCHEDULER_TIME": str(time.time() + 5)
        })

    collection = worker.mongodb_jobs.tests_inserts
    assert collection.find().count() == 0

    # It should be done a first time immediately
    time.sleep(3)
    inserts = list(collection.find())
    assert len(inserts) == 1
    print inserts
    assert collection.find({"params.weekday": datetime.datetime.utcnow().weekday()}).count() == 1


def test_scheduler_weekday_dailytime(worker):
    # Task is scheduled in 5 seconds
    worker.start(
        flags="--scheduler --config tests/fixtures/config-scheduler5.py",
        env={
            # We need to pass this in the environment so that each worker has the
            # exact same hash
            "MRQ_TEST_SCHEDULER_TIME": str(time.time() + 5)
        })

    collection = worker.mongodb_jobs.tests_inserts
    assert collection.find().count() == 0

    # Should be launched a first time
    time.sleep(2)
    assert len(list(collection.find())) == 2

    # the dailytime passes
    time.sleep(7)
    inserts = list(collection.find())
    assert len(inserts) == 3
    print inserts
    assert collection.find({"params.weekday": datetime.datetime.utcnow().weekday(), "params.later": False}).count() == 2

    # more time passes and we do nothing
    time.sleep(7)
    inserts = list(collection.find())
    assert len(inserts) == 3
    print inserts
    assert collection.find({"params.weekday": datetime.datetime.utcnow().weekday(), "params.later": False}).count() == 2

