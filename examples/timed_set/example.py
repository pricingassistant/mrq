from mrq.task import Task
from datetime import datetime
import mrq.context as context
import time


class Print(Task):

    def run(self, params):

        print "Hello World"

        last_task = context.connections.redis.get("test:print")
        if last_task:
          print "Last task was executed %.2f seconds ago" % (time.time() - float(last_task))

        context.connections.redis.set("test:print", time.time())

        print "Given params test is", params["test"]

        print "Bye"


class RemoveRedisEntry(Task):

    def run(self, params):

        context.connections.redis.delete("test:print")