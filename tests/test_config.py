

def test_config(worker):
  """ Test different config passing options. """

  worker.start()

  cfg = worker.send_task("mrq.basetasks.tests.general.GetConfig", {}, block=True)

  assert "mongodb_jobs" in cfg
  assert cfg.get("additional_unexpected_config") is None

  worker.stop()

  worker.start(flags="--config tests/fixtures/config2.py")

  cfg = worker.send_task("mrq.basetasks.tests.general.GetConfig", {}, block=True)

  assert cfg["name"] == "testworker"
  assert cfg.get("additional_unexpected_config") == "1"

  worker.stop()

  worker.start(flags="--config tests/fixtures/config2.py --name xxx")

  cfg = worker.send_task("mrq.basetasks.tests.general.GetConfig", {}, block=True)

  assert cfg["name"] == "xxx"
  assert cfg.get("additional_unexpected_config") == "1"

  worker.stop()
