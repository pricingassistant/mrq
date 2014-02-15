from time import sleep
from mrq.task import Task
from mrq.logger import log


class Add(Task):

  def run(self, params):

    log.info("adding", params)
    res = params.get("a", 0) + params.get("b", 0)

    if params.get("sleep", 0):
      log.info("sleeping", params.get("sleep", 0))
      sleep(params.get("sleep", 0))

    return res


class RaiseException(Task):

  def run(self, params):

    sleep(params.get("sleep", 0))

    raise Exception(params.get("message", ""))


class ReturnParams(Task):

  def run(self, params):

    sleep(params.get("sleep", 0))

    return params
