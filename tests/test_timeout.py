

def test_timeout_normal(worker):

    worker.start(flags="--config tests/fixtures/config1.py")

    r = worker.send_task(
        "tests.tasks.general.TimeoutFromConfig", {"a": 1, "b": 2}, block=True)
    assert r == 3

    r = worker.send_task("tests.tasks.general.TimeoutFromConfig", {
                         "a": 1, "b": 2, "sleep": 1000}, block=True, accept_statuses=["timeout"])
    assert r != 3


def test_timeout_global_config(worker):

    worker.start(env={"MRQ_DEFAULT_JOB_TIMEOUT": "1"})

    assert worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1, "sleep": 0}) == 42
    assert worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 1, "sleep": 2}, block=True, accept_statuses=["timeout"]) != 42
