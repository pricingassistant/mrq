from bson import ObjectId
import urllib2
import json
import time
import os
import pytest


@pytest.mark.parametrize(["use_worker"], [[False], [True]])
def test_subpool_simple(worker, use_worker):

    # Check that a subpool can be used both in an outside of a Job context
    if use_worker:
        worker.start()
    else:
        from tests.tasks.general import SubPool

    def run(params):
        if use_worker:
            return worker.send_task("tests.tasks.general.SubPool", params)
        else:
            return SubPool().run(params)

    # Check that sequential sleeps work
    start_time = time.time()
    result = run({
        "pool_size": 1, "inner_params": [1, 1]
    })
    total_time = time.time() - start_time

    assert result == [1, 1]
    assert total_time > 2

    # py.test doesn't use gevent so we don't get the benefits of the hub
    if use_worker:

        # Parallel sleeps
        start_time = time.time()
        result = run({
            "pool_size": 20, "inner_params": [1] * 20
        })
        total_time = time.time() - start_time

        assert result == [1] * 20
        assert total_time < 2


def test_subpool_exception(worker):

    # Exception
    worker.send_task("tests.tasks.general.SubPool", {
        "pool_size": 20, "inner_params": ["exception"]
    }, accept_statuses=["failed"])


@pytest.mark.parametrize(["p_size"], [
    [0],
    [1],
    [2],
    [100]
])
def test_subpool_import(worker, p_size):
    """ This tests that the patch_import() function does its job of preventing a gevent crash
    like explained in https://code.google.com/p/gevent/issues/detail?id=108 """

    # Large file import
    worker.send_task("tests.tasks.general.SubPool", {
        "pool_size": p_size, "inner_params": ["import-large-file"] * p_size
    }, accept_statuses=["success"])
