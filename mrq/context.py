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
  from mongokit import ReplicaSetConnection as MongoReplicaSetClient
except ImportError:
  from pymongo import MongoClient
  from pymongo import MongoReplicaSetClient


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

  if config["trace_mongodb"] or config["print_mongodb"]:
    from mrq.monkey import patch_pymongo
    patch_pymongo(config)

  if config["add_latency"]:
    from mrq.monkey import patch_network_latency
    patch_network_latency(config["add_latency"])

  if not config["no_import_patch"]:
    from mrq.monkey import patch_import
    patch_import()


def get_current_config():
  global _CONFIG
  return _CONFIG


def retry_current_job(**kwargs):
  get_current_job().retry(**kwargs)


def _connections_factory(attr):

  config = get_current_config()

  # Connection strings may be stored directly in config
  config_obj = config.get(attr)

  if attr.startswith("redis"):

    if type(config_obj) in [str, unicode]:

      urlparse.uses_netloc.append('redis')
      redis_url = urlparse.urlparse(config_obj)

      log.info("%s: Connecting to Redis at %s..." % (attr, redis_url.hostname))

      redis_pool = pyredis.ConnectionPool(
        host=redis_url.hostname,
        port=redis_url.port,
        db=int((redis_url.path or "").replace("/", "") or "0"),
        password=redis_url.password
      )
      return pyredis.StrictRedis(connection_pool=redis_pool)

    # Let's just assume we got a StrictRedis-like object!
    else:
      return config_obj

  elif attr.startswith("mongodb"):

    if type(config_obj) in [str, unicode]:

      (mongoAuth, mongoUsername, mongoPassword, mongoHosts, mongoDbName, mongoDbOptions) = re.match(
        r"mongodb://((\w+):(\w+)@)?([\w\.:,-]+)/([\w-]+)(\?.*)?", config_obj).groups()

      log.debug("%s: Connecting to MongoDB at %s/%s..." % (attr, mongoHosts, mongoDbName))

      kwargs = {"use_greenlets": True}
      options = {}
      if mongoDbOptions:
        options = {k: v[0] for k, v in urlparse.parse_qs(mongoDbOptions[1:]).iteritems()}

      # We automatically switch to MongoReplicaSetClient when getting a replicaSet option.
      # This should cover most use-cases.
      # http://api.mongodb.org/python/current/examples/high_availability.html#mongoreplicasetclient
      if options.get("replicaSet"):
        db = MongoReplicaSetClient(config_obj, **kwargs)[mongoDbName]
      else:
        db = MongoClient(config_obj, **kwargs)[mongoDbName]
      log.debug("%s: ... connected." % (attr))

      return db

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


def progress(ratio, save=False):
  job = get_current_job()
  if not job:
    return
  job.set_progress(ratio, save=save)


def metric(name, incr=1, **kwargs):
  cfg = get_current_config()
  if cfg.get("metric_hook"):
    return cfg.get("metric_hook")(name, incr=incr, **kwargs)

