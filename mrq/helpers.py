""" Helpers are util functions which use the context """
from mrq.context import connections
import time


def ratelimit(key, limit, per=1, redis=None):
  """ Returns an integer with the number of available actions for the
  current period. If zero, rate was already reached. """

  if redis is None:
    redis = connections.redis

  # http://redis.io/commands/INCR
  now = int(time.time())

  k = "ratelimit:%s:%s" % (key, now // per)

  with redis.pipeline(transaction=True) as pipeline:
    pipeline.incr(k, 1)
    pipeline.expire(k, 10)
    value = pipeline.execute()

  current = int(value[0])

  if current >= limit:
    return 0
  else:
    return limit - current
