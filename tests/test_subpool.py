from bson import ObjectId
import urllib2
import json
import time
import os
import pytest


def test_subpool_simple(worker):

    worker.start()

    # Check that sequential sleeps work
    start_time = time.time()
    result = worker.send_task("tests.tasks.general.SubPool", {
        "pool_size": 1, "inner_params": [1, 1]
    })
    total_time = time.time() - start_time

    assert result == [1, 1]
    assert total_time > 2

    # Parallel sleeps
    start_time = time.time()
    result = worker.send_task("tests.tasks.general.SubPool", {
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
