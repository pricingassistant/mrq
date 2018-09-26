
def test_job_queue(worker):
  from mrq.context import connections
  worker.start()
  worker.send_task("tests.tasks.general.Wait", {}, block=False)
  assert connections.redis.get("queuesize:%s" % "default") == "1"
  worker.wait_for_idle()
  assert connections.redis.get("queuesize:%s" % "default") == "0"

def test_job_failed(worker):
  from mrq.context import connections
  worker.start()
  worker.send_task("tests.tasks.general.RaiseException", {}, block=False)
  worker.wait_for_idle()
  assert connections.redis.get("queuesize:%s" % "default") == "0"

def test_job_requeue(worker):
  from mrq.context import connections
  from mrq.job import Job

  worker.start()
  job_id = worker.send_task("tests.tasks.general.RaiseException", {}, block=False)
  worker.wait_for_idle()
  assert connections.redis.get("queuesize:%s" % "default") == "0"

  Job(job_id).requeue()
  assert connections.redis.get("queuesize:%s" % "default") == "1"
