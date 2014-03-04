
class Task(object):

  cancel_on_retry = False
  cancel_on_exception = False

  def __init__(self):
    pass

  def run(self, params):
    raise NotImplementedError
