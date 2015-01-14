from gevent import GreenletExit
import traceback
import sys


# Inherits from BaseException to avoid being caught when not intended.
class TimeoutInterrupt(BaseException):
    pass


class RetryInterrupt(BaseException):
    delay = None
    queue = None
    retry_count = 0
    original_exception = None

    def __str__(self):
        s = "<RetryInterrupt #%s: %s seconds, %s queue>" % (self.retry_count, self.delay, self.queue)
        if self.original_exception is not None:
            s += "\n---- Original exception: -----\n%s" % ("".join(traceback.format_exception(*self.original_exception)))

        return s


class MaxRetriesInterrupt(BaseException):
    pass


class StopRequested(GreenletExit):
    """ Thrown in the mail greenlet to stop dequeuing jobs. """
    pass


class JobInterrupt(GreenletExit):
    """ Interrupts that stop a job in its execution, e.g. when responding to a SIGTERM. """
    pass
