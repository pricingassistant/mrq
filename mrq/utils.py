import re
import importlib
import time


def group_iter(iterator, n=2):
  """ Given an iterator, it returns sub-lists made of n items.
  (except the last that can have len < n)

  """
  accumulator = []
  for item in iterator:
    accumulator.append(item)
    if len(accumulator) == n:
      yield accumulator
      accumulator = []

  # Yield what's left
  if len(accumulator) != 0:
    yield accumulator


def load_task_class(taskpath):
  """ Given a taskpath, returns the main task class. """

  return getattr(importlib.import_module(re.sub(r"\.[^.]+$", "", taskpath)), re.sub(r"^.*\.", "", taskpath))


# http://code.activestate.com/recipes/576655-wait-for-network-service-to-appear/
def wait_for_net_service(server, port, timeout=None):
  """ Wait for network service to appear
      @param timeout: in seconds, if None or 0 wait forever
      @return: True of False, if timeout is None may return only True or
               throw unhandled network exception
  """
  import socket
  import errno

  s = socket.socket()
  if timeout:
    from time import time as now
    # time module is needed to calc timeout shared between two exceptions
    end = now() + timeout

  while True:
    try:
      if timeout:
        next_timeout = end - now()
        if next_timeout < 0:
          return False
        else:
          s.settimeout(next_timeout)

      s.connect((server, port))

    except socket.timeout, err:
      # this exception occurs only if timeout is set
      if timeout:
        return False

    except Exception, err:
      # catch timeout exception from underlying network library
      # this one is different from socket.timeout
      if type(err.args) != tuple or err[0] != errno.ETIMEDOUT:
        pass  # raise
    else:
      s.close()
      return True
    time.sleep(0.1)
