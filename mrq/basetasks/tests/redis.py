from time import sleep
from mrq.task import Task
from mrq.context import log, connections
import urllib2


class MultiRedis(Task):

  def run(self, params):

    connections.redis.set("test", "xxx")
    connections.redis_second.set("test", "yyy")

    assert connections.redis.get("test") == "xxx"
    assert connections.redis_second.get("test") == "yyy"

    return "ok"
