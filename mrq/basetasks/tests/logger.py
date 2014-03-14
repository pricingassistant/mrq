from mrq.task import Task
from mrq.context import log


class Simple(Task):

  def run(self, params):

    if params["class_name"] == "unicode":
      log.info(u"caf\xe9")
    elif params["class_name"] == "string":
      log.info("cafe")
    elif params["class_name"] == "latin-1":
      log.info("caf\xe9")

    return True
