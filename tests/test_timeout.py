import time


def test_timeout_normal(worker):

    worker.start(flags="--config tests/fixtures/config1.py")

    r = worker.send_task(
        "tests.tasks.general.TimeoutFromConfig", {"a": 1, "b": 2}, block=True)
    assert r == 3

    r = worker.send_task("tests.tasks.general.TimeoutFromConfig", {
                         "a": 1, "b": 2, "sleep": 1000}, block=True, accept_statuses=["timeout"])
    assert r != 3

    r = worker.send_task("tests.tasks.general.TimeoutFromConfig", {
                         "a": 1, "b": 2, "sleep": 1000, "broadexcept": True}, block=True, accept_statuses=["timeout"])
    assert r != 3

def test_timeout_global_config(worker):

    worker.start(env={"MRQ_DEFAULT_JOB_TIMEOUT": "1"})

    assert worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1, "sleep": 0}) == 42
    assert worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1, "sleep": 2}, block=True, accept_statuses=["timeout"]) != 42


def test_timeout_subpool(worker):

    worker.start(env={"MRQ_DEFAULT_JOB_TIMEOUT": "1"})

    jobs = worker.send_task("tests.tasks.general.ListJobsByGreenlets", {}, block=True)

    other_jobs = [j for j in jobs["job_ids"] if j != jobs["current_job_id"]]
    assert len(other_jobs) == 0
    # Only one greenlet in current job
    assert jobs["job_ids"] == [jobs["current_job_id"]]

    r = worker.send_task("tests.tasks.general.SubPool", {
        "pool_size": 3,
        "inner_params": [1000, 1001, 1002, 1003, 1004]
    }, block=True, accept_statuses=["timeout"])
    assert r is None

    time.sleep(1)

    jobs = worker.send_task("tests.tasks.general.ListJobsByGreenlets", {}, block=True)

    other_jobs = [j for j in jobs["job_ids"] if j != jobs["current_job_id"]]
    print("Leftover greenlets: %s" % other_jobs)
    assert len(other_jobs) == 0
    # Only one greenlet in current job
    assert jobs["job_ids"] == [jobs["current_job_id"]]
