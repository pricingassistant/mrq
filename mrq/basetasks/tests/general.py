from time import sleep
from mrq.task import Task
from mrq.logger import log
import urllib2


class Add(Task):

  def run(self, params):

    log.info("adding", params)
    res = params.get("a", 0) + params.get("b", 0)

    if params.get("sleep", 0):
      log.info("sleeping", params.get("sleep", 0))
      sleep(params.get("sleep", 0))

    return res


class Fetch(Task):
  def run(self, params):

    f = urllib2.urlopen(params.get("url"))
    t = f.read()
    f.close()

    return len(t)


class RaiseException(Task):

  def run(self, params):

    sleep(params.get("sleep", 0))

    raise Exception(params.get("message", ""))


class ReturnParams(Task):

  def run(self, params):

    sleep(params.get("sleep", 0))

    return params
