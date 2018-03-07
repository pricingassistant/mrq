import time
import pytest
import os

OPTS = []
for cls in ["string", "unicode", "latin-1", "bytes1"]:
    for utf8_sys_stdout in [True, False]:
        OPTS.append([cls, utf8_sys_stdout])


@pytest.mark.parametrize(["class_name", "utf8_sys_stdout"], OPTS)
def test_supports_string_and_unicode(worker, class_name, utf8_sys_stdout):
    result = worker.send_task("tests.tasks.logger.Simple", {
        "class_name": class_name,
        "utf8_sys_stdout": utf8_sys_stdout
    })

    # Force-flush the logs
    worker.stop(deps=False)
    assert result

    time.sleep(0.1)

    # Job logs
    db_logs = list(worker.mongodb_logs.mrq_logs.find({"job": {"$exists": True}}))

    assert len(db_logs) == 1
    if class_name == "unicode":
        assert u"caf\xe9" in db_logs[0]["logs"]
    elif class_name == "string":
        assert u"cafe" in db_logs[0]["logs"]
    elif class_name == "latin-1":
        assert "caf" in db_logs[0]["logs"]
        assert u"cafe" not in db_logs[0]["logs"]
        assert u"caf\xe9" not in db_logs[0]["logs"]

    # Worker logs
    # db_logs = list(worker.mongodb_logs.mrq_logs.find({"worker": db_workers[0]["_id"]}))
    # assert len(db_logs) >= 1
    # if class_name == "unicode":
    #   assert u"caf\xe9" in db_logs
    # else:
    #   assert u"cafe" in db_logs

    worker.stop_deps()

def test_other_handlers(worker):
    worker.start(flags="--config tests/fixtures/config-logger.py")
    worker.send_task("tests.tasks.logger.Simple", {
        "class_name": "string"
    })
    worker.stop(deps=False)
    assert os.path.isfile("/tmp/mrq.log")
    worker.stop_deps()
    os.unlink("/tmp/mrq.log")

def test_log_level(worker):
    worker.start(flags="--log_level INFO --config tests/fixtures/config-logger.py")
    worker.send_task("tests.tasks.logger.Simple", {
        "class_name": "string"
    }, block=True)
    worker.stop(deps=False)
    assert os.path.isfile("/tmp/mrq.log")
    with open("/tmp/mrq.log") as f:
        lines = f.readlines()
        assert all(["DEBUG" not in line for line in lines])
    worker.stop_deps()
    os.unlink("/tmp/mrq.log")


def test_collection_is_capped(worker):
    result = worker.send_task("tests.tasks.logger.Simple", {
        "class_name": "string"
    })

    # Force-flush the logs
    worker.stop(deps=False)
    assert worker.mongodb_logs.mrq_logs.options()["capped"] is True

    worker.stop_deps()