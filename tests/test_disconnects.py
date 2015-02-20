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

    job_id1 = worker.send_task("tests.tasks.general.Add", {
                               "a": 41, "b": 1, "sleep": 5}, block=False, queue="default")

    time.sleep(2)

    service.stop()
    service.start()

    service_pid2 = service.process.pid

    # Make sure we did restart
    assert service_pid != service_pid2

    time.sleep(5)

    # Result should be there without issues
    assert Job(job_id1).fetch().data["result"] == 42


@pytest.mark.parametrize(["gevent_count", "subpool_size", "iterations", "expected_clients"], [
    (None, None, 1, 1),     # single task opens a single connection
    (None, None, 2, 1),
    (None, 10, 1, 10),      # single task with subpool of 10 opens 10 connections
    (None, 10, 2, 10),
    (None, 200, 1, 100),    # single task with subpool of 200 opens 100 connections, we reach the max_connections limit
    (None, 200, 2, 100),
    (4, None, 1, 4),        # 4 gevent workers with a single task each : 4 connections
    (4, None, 2, 4),
    (2, 2, 1, 4),           # 2 gevent workers with 2 single tasks each : 4 connections
    (2, 2, 2, 4),

])
def test_redis_disconnections(gevent_count, subpool_size, iterations, expected_clients, worker):
    """ mrq.context.connections is not the actual connections pool that the worker uses.
        this worker's pool is not accessible from here, since it runs in a different thread.
    """
    from mrq.context import connections

    worker.start_deps()

    gevent_count = gevent_count if gevent_count is not None else 1

    get_clients = lambda: [c for c in connections.redis.client_list() if c.get("cmd") != "client"]

    assert len(get_clients()) == 0

    # 1. start the worker and asserts that there is a redis client connected
    kwargs = {"flags": "--redis_max_connections 100", "deps": False}
    if gevent_count:
        kwargs["flags"] += " --gevent %s" % gevent_count

    worker.start(**kwargs)

    for i in range(0, iterations):
        # sending tasks has the good side effect to wait for the worker to connect to redis
        worker.send_tasks("tests.tasks.redis.Disconnections", [{"subpool_size": subpool_size}] * gevent_count)

    assert len(get_clients()) == expected_clients

    # 2. kill the worker and make sure that the connection was closed
    worker.stop(deps=False)  # so that we still have access to redis

    assert len(get_clients()) == 0

    worker.stop_deps()
