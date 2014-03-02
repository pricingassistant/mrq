from .logger import LoggerInterface
import gevent
import urlparse
import redis as pyredis
import re
import time
from .utils import LazyObject

try:
  # MongoKit's Connection object is just a wrapped MongoClient.
  from mongokit import Connection as MongoClient
except:
  from pymongo import MongoClient


# greenletid => Job object
_GREENLET_JOBS_REGISTRY = {}

_WORKER = None

_CONFIG = None

# Global log object, usable from all tasks
log = LoggerInterface(None, job="current")


def set_current_job(job):
  current = gevent.getcurrent()
  _GREENLET_JOBS_REGISTRY[id(current)] = job

  current.__dict__["_trace_time"] = 0
  current.__dict__["_trace_switches"] = 0


def get_current_job(greenlet_id=None):
  if greenlet_id is None:
    greenlet_id = id(gevent.getcurrent())
  return _GREENLET_JOBS_REGISTRY.get(greenlet_id)


def set_current_worker(worker):
  global _WORKER
  _WORKER = worker
  set_current_config(worker.config)


def get_current_worker():
  global _WORKER
  return _WORKER


def set_current_config(config):
  global _CONFIG
  _CONFIG = config
  log.quiet = config["quiet"]


def get_current_config():
  global _CONFIG
  return _CONFIG


def retry_current_job(**kwargs):
  get_current_job().retry(**kwargs)


def _connections_factory(attr):

  config = get_current_config()

  # Connection strings may be stored directly in config
  config_obj = config.get(attr)

  if attr == "redis":

    if type(config_obj) in [str, unicode]:

      urlparse.uses_netloc.append('redis')
      redis_url = urlparse.urlparse(config_obj)

      log.info("Connecting to Redis at %s..." % redis_url.hostname)

      redis_pool = pyredis.ConnectionPool(
        host=redis_url.hostname,
        port=redis_url.port,
        db=0,
        password=redis_url.password
      )
      return pyredis.StrictRedis(connection_pool=redis_pool)

    # Let's just assume we got a StrictRedis-like object!
    else:
      return config_obj

  elif attr.startswith("mongodb"):

    if type(config_obj) in [str, unicode]:

      (mongoAuth, mongoUsername, mongoPassword, mongoHosts, mongoDbName) = re.match(
        "mongodb://((\w+):(\w+)@)?([\w\.:,-]+)/([\w-]+)", config_obj).groups()

      log.debug("Connecting to MongoDB at %s/%s..." % (mongoHosts, mongoDbName))

      value = MongoClient(mongoHosts)[mongoDbName]
      if mongoUsername:
        value.authenticate(mongoUsername, mongoPassword)

      return value

    # Let's just assume we got a MongoDB-like object!
    else:
      return config_obj

connections = LazyObject()
connections.add_factory(_connections_factory)
del _connections_factory


def enable_greenlet_tracing():

  # Tracing seems to cause a 2-5% performance loss.

  import greenlet
  greenlet.GREENLET_USE_TRACING = True

  def trace(*args):

    time_since_last_switch = time.time() - trace.last_switch

    # Record the time of the current switch
    trace.last_switch = time.time()

    if args[0] == "switch":
      # We are switching from the greenlet args[1][0] to the greenlet args[1][1]
      args[1][0].__dict__.setdefault("_trace_time", 0)
      args[1][0].__dict__["_trace_time"] += time_since_last_switch
      args[1][0].__dict__.setdefault("_trace_switches", 0)
      args[1][0].__dict__["_trace_switches"] += 1

    elif args[0] == "throw":
      pass

  trace.last_switch = time.time()

  greenlet.settrace(trace)
