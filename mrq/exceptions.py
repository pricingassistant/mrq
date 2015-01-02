from gevent import GreenletExit


# Inherits from BaseException to avoid being caught when not intended.
class TimeoutInterrupt(BaseException):
    pass


class RetryInterrupt(BaseException):
    delay = None
    queue = None
    retry_count = 0

    def __str__(self):
        return "<RetryInterrupt #%s: %s seconds, %s queue>" % (self.retry_count, self.delay, self.queue)


class MaxRetriesInterrupt(BaseException):
    pass


class StopRequested(GreenletExit):
    """ Thrown in the mail greenlet to stop dequeuing jobs. """
    pass


class JobInterrupt(GreenletExit):
    """ Interrupts that stop a job in its execution, e.g. when responding to a SIGTERM. """
    pass
