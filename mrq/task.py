
class Task(object):

  cancel_on_retry = False
  cancel_on_exception = False
  cancel_on_timeout = False

  # Are we the first task that a Job called?
  is_main_task = False

  def __init__(self):
    pass

  def run(self, params):
    raise NotImplementedError
