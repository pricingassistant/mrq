from mrq.task import Task
from mrq.context import connections, subpool_map


class MultiRedis(Task):

    def run(self, params):

        connections.redis.set("test", "xxx")
        connections.redis_second.set("test", "yyy")

        assert connections.redis.get("test") == "xxx"
        assert connections.redis_second.get("test") == "yyy"

        return "ok"


class Disconnections(Task):

    def run(self, params):

        def inner(i):
            return connections.redis.get("test")

        if params["subpool_size"]:
          subpool_map(params["subpool_size"], inner, range(0, params["subpool_size"] * 5))
        else:
          inner(0)
