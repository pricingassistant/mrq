import time
from mrq.task import Task
from mrq.context import log


class Locked(Task):

    locked_job = True

    def run(self, params):
      time.sleep(1)
