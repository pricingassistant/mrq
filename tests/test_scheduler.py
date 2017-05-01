from __future__ import print_function
from builtins import str
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

    time.sleep(1)

    # There are 4 test tasks with 5 second interval
    inserts = list(collection.find())
    assert len(inserts) == 4

    jobs = list(scheduled_jobs.find())
    assert len(jobs) == 4

    time.sleep(6)

    # They should have ran again.
    inserts = list(collection.find())
    assert len(inserts) == 8

    worker.stop(deps=False)

    collection.remove({})

    scheduled_jobs.update_many({}, {"$set": {"datelastqueued": datetime.datetime.utcnow()}})

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
    print(inserts)
    assert len(inserts) == 3, inserts


@pytest.mark.parametrize(["p_flags"], PROCESS_CONFIGS)
def test_scheduler_dailytime(worker, p_flags):

    now = time.time()

    # Task is scheduled in 3 seconds
    def _start(deps=True):
        worker.start(
            flags="--scheduler --config tests/fixtures/config-scheduler3.py %s" % p_flags,
            deps=deps,
            env={
                # We need to pass this in the environment so that each worker has the
                # exact same hash
                "MRQ_TEST_SCHEDULER_TIME": str(now + 10)
            })

    _start(deps=True)

    collection = worker.mongodb_jobs.tests_inserts

    # It should NOT be done a first time immediately
    time.sleep(4)
    inserts = list(collection.find())
    assert len(inserts) == 0
    print(inserts)
    assert collection.find({"params.b": "test"}).count() == 0

    # Only when the dailytime passes
    time.sleep(7)
    assert collection.find().count() == 2
    assert collection.find({"params.b": "test"}).count() == 1

    # Nothing more should happen today
    time.sleep(5)
    assert collection.find().count() == 2
    assert collection.find({"params.b": "test"}).count() == 1

    # .. even if we restart
    worker.stop(deps=False)

    time.sleep(1)

    _start(deps=False)

    time.sleep(5)

    assert collection.find().count() == 2
    assert collection.find({"params.b": "test"}).count() == 1


def test_scheduler_weekday_dailytime(worker):
    # Task is scheduled in 5 seconds
    worker.start(
        flags="--scheduler --config tests/fixtures/config-scheduler5.py",
        env={
            # We need to pass this in the environment so that each worker has the
            # exact same hash
            "MRQ_TEST_SCHEDULER_TIME": str(time.time() + 10)
        })

    collection = worker.mongodb_jobs.tests_inserts

    # Should not be launched a first time
    time.sleep(5)
    assert len(list(collection.find())) == 0

    # the dailytime passes
    time.sleep(7)
    inserts = list(collection.find())
    assert len(inserts) == 1
    print(inserts)
    assert collection.find({"params.weekday": datetime.datetime.utcnow().weekday(), "params.later": False}).count() == 1

    # more time passes and we do nothing
    time.sleep(7)
    inserts = list(collection.find())
    assert len(inserts) == 1
    print(inserts)
    assert collection.find({"params.weekday": datetime.datetime.utcnow().weekday(), "params.later": False}).count() == 1


def test_scheduler_monthday(worker):
    # Task is scheduled in 3 seconds
    worker.start(
        flags="--scheduler --config tests/fixtures/config-scheduler6.py",
        env={
            # We need to pass this in the environment so that each worker has the
            # exact same hash
            "MRQ_TEST_SCHEDULER_TIME": str(time.time() + 10)
        })

    collection = worker.mongodb_jobs.tests_inserts

    # It should not be done a first time immediately
    time.sleep(5)
    inserts = list(collection.find())
    assert len(inserts) == 0

    # And then only once
    time.sleep(6)
    inserts = list(collection.find())
    assert len(inserts) == 1
    assert collection.find({"params.monthday": datetime.datetime.utcnow().day}).count() == 1


def test_scheduler_noparams(worker):
    # Task is scheduled in 3 seconds
    worker.start(
        flags="--scheduler --config tests/fixtures/config-scheduler7.py"
    )

    time.sleep(2)

    scheduled_jobs = worker.mongodb_jobs.mrq_scheduled_jobs
    jobs = worker.mongodb_jobs.mrq_jobs

    assert len(list(scheduled_jobs.find())) == 3
    assert len(list(jobs.find())) == 3


@pytest.mark.parametrize(["p_config"], [
    ["config-scheduler-invalid1.py"],
    ["config-scheduler-invalid2.py"],
    ["config-scheduler-invalid3.py"],
    ["config-scheduler-invalid4.py"]
])
def test_scheduler_invalidconfig(worker, p_config):
    # Task is scheduled in 3 seconds
    worker.start(flags="--scheduler --config tests/fixtures/%s" % p_config, block=False)

    time.sleep(3)

    collection = worker.mongodb_jobs.tests_inserts
    jobs = worker.mongodb_jobs.mrq_jobs

    # The worker should be stopped immediately and do not jobs

    assert collection.count() == 0
    assert jobs.count() == 0

    worker.process.wait()
    assert worker.process.returncode > 0

    worker.stop_deps()
