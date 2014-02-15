
class Task(object):

  drop_on_retry = False
  drop_on_exception = False

  def __init__(self, job=None):
    self.job = job

  def run(self, params):
    raise NotImplementedError
