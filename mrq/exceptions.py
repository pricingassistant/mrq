from gevent import GreenletExit


# Inherits from BaseException to avoid being caught when not intended.
class JobTimeoutException(BaseException):
  pass


# Inherits from BaseException to avoid being caught when not intended.
class RetryInterrupt(BaseException):
  countdown = None
  queue = None


class StopRequested(GreenletExit):
  """ Thrown in the mail greenlet to stop dequeuing jobs. """
  pass


class JobInterrupt(GreenletExit):
  """ Interrupts that stop a job in its execution, e.g. when responding to a SIGTERM. """
  pass
