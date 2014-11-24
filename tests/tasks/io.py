from mrq.task import Task
from mrq.context import connections
import urllib2


class TestIo(Task):

    def run(self, params):

        # from mrq.monkey import patch_network_latency

        # patch_network_latency(2)

        if params["test"] == "mongodb-insert":

            return connections.mongodb_logs.tests_inserts.insert({"params": params["params"]}, manipulate=False)

        elif params["test"] == "mongodb-find":

            cursor = connections.mongodb_logs.tests_inserts.find({"test": "x"})
            return list(cursor)

        elif params["test"] == "mongodb-count":

            return connections.mongodb_logs.tests_inserts.count()

        elif params["test"] == "redis-llen":

            return connections.redis.llen(params["params"]["key"])

        elif params["test"] == "redis-lpush":

            return connections.redis.lpush(params["params"]["key"], "xxx")

        elif params["test"] == "urllib2-get":

            fp = urllib2.urlopen(params["params"]["url"])
            print fp.read
            return fp.read()

        elif params["test"] == "urllib2-post":

            return urllib2.urlopen(params["params"]["url"], data="x=x").read()


