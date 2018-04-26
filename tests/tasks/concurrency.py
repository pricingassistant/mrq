import time
from mrq.task import Task
from mrq.context import log
from .general import Add

class LockedAdd(Add):

    max_concurrency = 1

    def run(self, params):
        log.info("adding", params)
        res = params.get("a", 0) + params.get("b", 0)

        if params.get("sleep", 0):
            log.info("sleeping %d", params.get("sleep", 0))
            time.sleep(params.get("sleep", 0))

        return res
