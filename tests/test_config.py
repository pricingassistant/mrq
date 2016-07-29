import json


def test_config(worker):
    """ Test different config passing options. """

    # Default config values
    worker.start()

    cfg = json.loads(
        worker.send_task("tests.tasks.general.GetConfig", {}, block=True))

    assert cfg["mongodb_jobs"] == "mongodb://127.0.0.1:27017/mrq"
    assert cfg.get("additional_unexpected_config") is None

    worker.stop()

    # Values from config file
    worker.start(flags="--config tests/fixtures/config2.py")

    cfg = json.loads(
        worker.send_task("tests.tasks.general.GetConfig", {}, block=True))

    assert cfg["mongodb_jobs"] == "mongodb://127.0.0.1:27017/mrq?connectTimeoutMS=4242"
    assert cfg["name"] == "testworker"
    assert cfg.get("additional_unexpected_config") == "1"

    worker.stop()

    # CLI > Config file && CLI > ENV
    worker.start(flags="--config tests/fixtures/config2.py --name xxx", env={"MRQ_NAME": "yyy"})

    cfg = json.loads(
        worker.send_task("tests.tasks.general.GetConfig", {}, block=True))

    assert cfg["name"] == "xxx"
    assert cfg.get("additional_unexpected_config") == "1"

    worker.stop()

    # ENV > Config file
    worker.start(flags="--config tests/fixtures/config2.py", env={"MRQ_NAME": "yyy"})

    cfg = json.loads(
        worker.send_task("tests.tasks.general.GetConfig", {}, block=True))

    assert cfg["name"] == "yyy"
    assert cfg.get("additional_unexpected_config") == "1"

    worker.stop()
