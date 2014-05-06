import re
import importlib
import time

""" Utils are functions that should be independent from the rest of MRQ's codebase """


def get_local_ip():
  """ Returns the local IP. Can be overwritten in the config with --local-ip so don't call
  this function directly, instead get the current value from the config """
  import socket
  return socket.gethostbyname(socket.gethostname())


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


# http://code.activestate.com/recipes/578231-probably-the-fastest-memoization-decorator-in-the-/
def memoize(f):
  """ Memoization decorator for a function taking one or more arguments. """
  class memodict(dict):
    def __getitem__(self, *key):
      return dict.__getitem__(self, key)

    def __missing__(self, key):
      ret = self[key] = f(*key)
      return ret

  return memodict().__getitem__


def memoize_single_argument(f):
  """ Memoization decorator for a function taking a single argument """
  class memodict(dict):
    def __missing__(self, key):
      ret = self[key] = f(key)
      return ret
  return memodict().__getitem__


@memoize_single_argument
def load_class_by_path(taskpath):
  """ Given a taskpath, returns the main task class. """

  return getattr(importlib.import_module(re.sub(r"\.[^.]+$", "", taskpath)), re.sub(r"^.*\.", "", taskpath))


def lazyproperty(fn):
  attr_name = '_lazy_' + fn.__name__

  @property
  def _lazyprop(self):
    if not hasattr(self, attr_name):
      setattr(self, attr_name, fn(self))
    return getattr(self, attr_name)
  return _lazyprop


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


class LazyObject(object):
  """ Lazy-connection class. Connections will only be initialized when first used. """

  def __init__(self):
    self._factories = []
    self._attributes_via_factories = []

  def add_factory(self, factory):
    self._factories.append(factory)

  # This will be called only once, when the attribute is still missing
  def __getattr__(self, attr):

    for factory in self._factories:
      value = factory(attr)
      if value is not None:
        self._attributes_via_factories.append(attr)
        self.__dict__[attr] = value
        return value

  def reset(self):
    # TODO proper connection close?
    for attr in self._attributes_via_factories:
      del self.__dict__[attr]
    self._attributes_via_factories = []
