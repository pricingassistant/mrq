
def test_context_get(worker):

  result = worker.send_task("tests.tasks.context.GetContext", {})

  assert result["job_id"]
  assert result["worker_id"]
  assert result["config"]["redis"]


def test_context_connections_redis(worker):

  worker.start(flags=" --config tests/fixtures/config-multiredis.py")

  assert worker.send_task("tests.tasks.redis.MultiRedis", {}) == "ok"
