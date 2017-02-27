""" Helpers are util functions which use the context """
from .context import connections, get_current_config
import time


def ratelimit(key, limit, per=1, redis=None):
    """ Returns an integer with the number of available actions for the
    current period in seconds. If zero, rate was already reached. """

    if redis is None:
        redis = connections.redis

    # http://redis.io/commands/INCR
    now = int(time.time())

    k = "ratelimit:%s:%s" % (key, now // per)

    with redis.pipeline(transaction=True) as pipeline:
        pipeline.incr(k, 1)
        pipeline.expire(k, per + 10)
        value = pipeline.execute()

    current = int(value[0]) - 1

    if current >= limit:
        return 0
    else:
        return limit - current


def metric(name, incr=1, **kwargs):
    cfg = get_current_config()
    if cfg.get("metric_hook"):
        return cfg.get("metric_hook")(name, incr=incr, **kwargs)
