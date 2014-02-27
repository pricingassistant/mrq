import time
from mrq.job import Job
import pytest


@pytest.mark.parametrize(["p_service"], [["mongodb"], ["redis"]])
def test_disconnects_service_during_task(worker, p_service):
  """ Test what happens when mongodb disconnects during a job
  """

  worker.start()

  if p_service == "mongodb":
    service = worker.fixture_mongodb
  elif p_service == "redis":
    service = worker.fixture_redis

  service_pid = service.process.pid

  job_id1 = worker.send_task("mrq.basetasks.tests.general.Add", {"a": 41, "b": 1, "sleep": 5}, block=False, queue="default")

  time.sleep(2)

  service.stop()
  service.start()

  service_pid2 = service.process.pid

  # Make sure we did restart
  assert service_pid != service_pid2

  time.sleep(5)

  # Result should be there without issues
  assert Job(job_id1).fetch().data["result"] == 42
