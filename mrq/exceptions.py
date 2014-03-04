from gevent import GreenletExit


# Inherits from BaseException to avoid being caught when not intended.
class JobTimeoutException(BaseException):
  pass


class CancelInterrupt(BaseException):
  pass


# Inherits from BaseException to avoid being caught when not intended.
class RetryInterrupt(BaseException):
  countdown = None
  queue = None

  def __str__(self):
    return "<RetryInterrupt %s seconds, %s queue>" % (self.countdown, self.queue)


class StopRequested(GreenletExit):
  """ Thrown in the mail greenlet to stop dequeuing jobs. """
  pass


class JobInterrupt(GreenletExit):
  """ Interrupts that stop a job in its execution, e.g. when responding to a SIGTERM. """
  pass
