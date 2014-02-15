from gevent import GreenletExit


# Inherits from BaseException to avoid being caught when not intended.
class JobTimeoutException(BaseException):
  pass


# Inherits from BaseException to avoid being caught when not intended.
class RetryInterrupt(BaseException):
  eta = 24 * 3600
  pass


class StopRequested(GreenletExit):
  pass
