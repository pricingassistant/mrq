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


def test_subpool_imap():

    from mrq.context import subpool_imap

    def iterator(n):
        for i in range(0, n):
            if i == 5:
                raise Exception("Iterator exception!")
            yield i

    def inner_func(i):
        time.sleep(1)
        print "inner_func: %s" % i
        if i == 4:
            raise Exception("Inner exception!")
        return i * 2

    with pytest.raises(Exception):
        for res in subpool_imap(10, inner_func, iterator(10)):
            print "Got %s" % res

    for res in subpool_imap(2, inner_func, iterator(1)):
        print "Got %s" % res

    with pytest.raises(Exception):
        for res in subpool_imap(2, inner_func, iterator(5)):
            print "Got %s" % res
