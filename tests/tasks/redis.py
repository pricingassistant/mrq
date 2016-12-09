from mrq.task import Task
from mrq.context import connections, subpool_map
import gevent
import time


class MultiRedis(Task):

    def run(self, params):

        connections.redis.set("test", "xxx")
        connections.redis_second.set("test", "yyy")

        assert connections.redis.get("test") == "xxx"
        assert connections.redis_second.get("test") == "yyy"

        return "ok"


class Disconnections(Task):

    def run(self, params):

        def get_clients():
            return [c for c in connections.redis.client_list() if c.get("cmd") != "client"]

        def inner(i):
            print "Greenlet #%s, %s clients so far" % (id(gevent.getcurrent()), len(get_clients()))
            return connections.redis.get("test")

        if params["subpool_size"]:
          subpool_map(params["subpool_size"], inner, range(0, params["subpool_size"] * 5))
        else:
          inner(0)
