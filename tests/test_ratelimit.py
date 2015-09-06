from mrq.helpers import ratelimit
import time


def test_helpers_ratelimit(worker):

    worker.start_deps()

    assert ratelimit("k3", 1, per=1) == 1
    assert ratelimit("k3", 1, per=1) == 0
    assert ratelimit("k3", 1, per=1) == 0

    for i in range(0, 10):
        r = ratelimit("k", 10, per=1)
        assert r == 10 - i

    assert ratelimit("k", 10, per=1) == 0
    assert ratelimit("k2", 5, per=1) == 5

    # We *could* have failures there if we go over a second but we've not seen
    # it much so far.
    for i in range(0, 100):
        assert ratelimit("k", 10, per=1) == 0

    # TODO: test the "per" argument a bit better.
    time.sleep(1)

    assert ratelimit("k", 10, per=1) == 10
    assert ratelimit("k", 10, per=1) == 9

    # This is actually another counter.
    assert ratelimit("k", 10, per=10) == 10
    assert ratelimit("k", 10, per=10) == 9

    worker.stop_deps()
