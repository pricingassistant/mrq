from future import standard_library
standard_library.install_aliases()
from mrq.task import Task
from mrq.context import connections, log
import urllib.request, urllib.error, urllib.parse
from future.moves.urllib.request import urlopen

class TestIo(Task):

    def run(self, params):

        log.info("I/O starting")
        ret = self._run(params)
        log.info("I/O finished")

        return ret

    def _run(self, params):

        if params["test"] == "mongodb-insert":

            return connections.mongodb_jobs.tests_inserts.insert({"params": params["params"]}, manipulate=False)

        elif params["test"] == "mongodb-find":

            cursor = connections.mongodb_jobs.tests_inserts.find({"test": "x"})
            return list(cursor)

        elif params["test"] == "mongodb-count":

            return connections.mongodb_jobs.tests_inserts.count()

        elif params["test"] == "mongodb-full-getmore":

            connections.mongodb_jobs.tests_inserts.insert_many([{"a": 1}, {"a": 2}])

            return list(connections.mongodb_jobs.tests_inserts.find(batch_size=1))

        elif params["test"] == "redis-llen":

            return connections.redis.llen(params["params"]["key"])

        elif params["test"] == "redis-lpush":

            return connections.redis.lpush(params["params"]["key"], "xxx")

        elif params["test"] == "urllib2-get":

            fp = urlopen(params["params"]["url"])
            return fp.read()

        elif params["test"] == "urllib2-post":

            return urlopen(params["params"]["url"], data="x=x").read()

        elif params["test"] == "requests-get":

            import requests
            return requests.get(params["params"]["url"], verify=False).text
