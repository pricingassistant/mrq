

def test_timeout_normal(worker):

    worker.start(flags="--config tests/fixtures/config1.py")

    r = worker.send_task(
        "tests.tasks.general.TimeoutFromConfig", {"a": 1, "b": 2}, block=True)
    assert r == 3

    r = worker.send_task("tests.tasks.general.TimeoutFromConfig", {
                         "a": 1, "b": 2, "sleep": 1000}, block=True, accept_statuses=["timeout"])
    assert r != 3


def test_timeout_cancel(worker):

    worker.start(flags="--config tests/fixtures/config1.py")

    r = worker.send_task("tests.tasks.general.TimeoutFromConfigAndCancel", {
                         "a": 1, "b": 2, "sleep": 1000}, block=True, accept_statuses=["cancel"])
    assert r != 3
