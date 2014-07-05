import time
import pytest


@pytest.mark.parametrize(["p_save"], [
  [True],
  [False]
])
def test_progress(worker, p_save):

  worker.start(flags="--report_interval 1")

  assert worker.send_task("tests.tasks.general.Progress", {"save": p_save}, block=False)

  time.sleep(5)

  assert worker.mongodb_jobs.mrq_jobs.find()[0]["progress"] > 0.2
  assert worker.mongodb_jobs.mrq_jobs.find()[0]["progress"] < 0.6

  time.sleep(5)

  worker.stop()

  assert worker.mongodb_jobs.mrq_jobs.find()[0]["progress"] == 1
