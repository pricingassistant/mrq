from gevent import GreenletExit
import traceback


# Inherits from BaseException to avoid being caught when not intended.
class _MrqInterrupt(BaseException):

    original_exception = None

    def _get_exception_name(self):
        return self.__class__.__name__

    def __str__(self):
        s = self._get_exception_name()
        if self.original_exception is not None:
            tb = "".join(traceback.format_exception(*self.original_exception))  # pylint: disable=not-an-iterable
            s += "\n---- Original exception: -----\n%s" % tb

        return s


class TimeoutInterrupt(_MrqInterrupt):
    pass


class AbortInterrupt(_MrqInterrupt):
    pass


class RetryInterrupt(_MrqInterrupt):
    delay = None
    queue = None
    retry_count = 0

    def _get_exception_name(self):
        return "%s #%s: %s seconds, %s queue" % (
            self.__class__.__name__, self.retry_count, self.delay, self.queue
        )


class MaxRetriesInterrupt(_MrqInterrupt):
    pass


class StopRequested(GreenletExit):
    """ Thrown in the mail greenlet to stop dequeuing jobs. """
    pass


class JobInterrupt(GreenletExit):
    """ Interrupts that stop a job in its execution, e.g. when responding to a SIGTERM. """
    pass


class MaxConcurrencyInterrupt(_MrqInterrupt):
    pass
