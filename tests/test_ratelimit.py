from mrq.helpers import ratelimit


def test_helpers_ratelimit(worker):

  worker.start_deps()

  for i in range(1, 10):
    r = ratelimit("k", 10, per=1)
    print "X", r, i
    assert r == 10 - i

  ratelimit("k", 10, per=1) == 9
  ratelimit("k2", 5, per=1) == 5

  for i in range(0, 100):
    assert ratelimit("k", 10, per=1) == 0

  # TODO: test the "per" argument a bit better.
