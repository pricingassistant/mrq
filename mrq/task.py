
class Task(object):

  drop_on_retry = False
  drop_on_exception = False

  def __init__(self):
    pass

  def run(self, params):
    raise NotImplementedError
