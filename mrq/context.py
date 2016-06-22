from future import standard_library
standard_library.install_aliases()
from builtins import next
from builtins import map
from past.builtins import basestring
from .logger import Logger
import gevent
import gevent.pool
import urllib.parse
import time
import pymongo
import traceback
from .utils import LazyObject, load_class_by_path
from itertools import count as itertools_count
from .config import get_config

# This should be MRQ's only Python object shared by all the jobs in the same process
_GLOBAL_CONTEXT = {

    # Contains all the running greenlets for this worker. greenletid => Job object
    "greenlets": {},

    # pointer to the current worker
    "worker": None,

    # pointer to the current config
    "config": {}
}

# Global log object, usable from all jobs
log = Logger(None, job="current")


def setup_context(**kwargs):
    """ Setup MRQ's environment.

        Note: gevent should probably be initialized too if you want to use concurrency.
    """
    set_current_config(get_config(**kwargs))


def set_current_job(job):
    current = gevent.getcurrent()

    current.__dict__["_trace_time"] = 0
    current.__dict__["_trace_switches"] = 0

    if job is None:
        if id(current) in _GLOBAL_CONTEXT["greenlets"]:
            del _GLOBAL_CONTEXT["greenlets"][id(current)]
    else:
        _GLOBAL_CONTEXT["greenlets"][id(current)] = job


def get_current_job(greenlet_id=None):
    if greenlet_id is None:
        greenlet_id = id(gevent.getcurrent())
    return _GLOBAL_CONTEXT["greenlets"].get(greenlet_id)


def set_current_worker(worker):
    _GLOBAL_CONTEXT["worker"] = worker


def get_current_worker():
    return _GLOBAL_CONTEXT["worker"]


def set_current_config(config):
    _GLOBAL_CONTEXT["config"] = config
    log.quiet = config["quiet"]

    if config["add_network_latency"] != "0" and config["add_network_latency"]:
        from mrq.monkey import patch_network_latency
        patch_network_latency(config["add_network_latency"])

    if config["print_mongodb"] or config["trace_io"]:
        from mrq.monkey import patch_pymongo
        patch_pymongo(config)

    if config["trace_io"]:
        from mrq.monkey import patch_io_all
        patch_io_all(config)

    if config["mongodb_logs"] == "0":
        log.handler.collection = False


def get_current_config():
    return _GLOBAL_CONTEXT["config"]


def retry_current_job(delay=None, max_retries=None, queue=None):
    current_job = get_current_job()
    if current_job:
        current_job.retry(delay=delay, max_retries=max_retries, queue=queue)


def abort_current_job():
    current_job = get_current_job()
    if current_job:
        current_job.abort()


def _connections_factory(attr):

    config = get_current_config()

    # Connection strings may be stored directly in config
    config_obj = config.get(attr)

    def versiontuple(v):
        return tuple(map(int, (v.split("."))))

    if attr.startswith("redis"):
        if isinstance(config_obj, basestring):

            import redis as pyredis

            urllib.parse.uses_netloc.append('redis')
            redis_url = urllib.parse.urlparse(config_obj)

            log.info("%s: Connecting to Redis at %s..." %
                     (attr, redis_url.hostname))

            redis_pool = pyredis.BlockingConnectionPool(
                host=redis_url.hostname,
                port=redis_url.port,
                db=int((redis_url.path or "").replace("/", "") or "0"),
                password=redis_url.password,
                max_connections=int(config.get("redis_max_connections")),
                timeout=int(config.get("redis_timeout")),
                decode_responses=True
            )
            return pyredis.StrictRedis(connection_pool=redis_pool)

        # Let's just assume we got a StrictRedis-like object!
        else:
            return config_obj

    elif attr.startswith("mongodb"):

        if isinstance(config_obj, basestring):

            if attr == "mongodb_logs" and config_obj == "1":
                return connections.mongodb_jobs
            elif config_obj == "0":
                return None

            from pymongo import MongoClient

            mongo_parsed = pymongo.uri_parser.parse_uri(config_obj)

            mongo_hosts = mongo_parsed["nodelist"]
            mongo_name = mongo_parsed["database"]

            log.debug("%s: Connecting to MongoDB at %s/%s..." % (attr, mongo_hosts, mongo_name))

            kwargs = {}

            db = MongoClient(config_obj, **kwargs)[mongo_name]

            log.debug("%s: ... connected. (readPreference=%s)" % (attr, db.read_preference))

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
            # We are switching from the greenlet args[1][0] to the greenlet
            # args[1][1]
            args[1][0].__dict__.setdefault("_trace_time", 0)
            args[1][0].__dict__["_trace_time"] += time_since_last_switch
            args[1][0].__dict__.setdefault("_trace_switches", 0)
            args[1][0].__dict__["_trace_switches"] += 1

        elif args[0] == "throw":
            pass

    trace.last_switch = time.time()

    greenlet.settrace(trace)  # pylint: disable=no-member


def subpool_map(pool_size, func, iterable):
    """ Starts a Gevent pool and run a map. Takes care of setting current_job and cleaning up. """

    if not pool_size:
        return [func(*args) for args in iterable]

    counter = itertools_count()

    current_job = get_current_job()

    def inner_func(*args):
        """ As each call to 'func' will be done in a random greenlet of the subpool, we need to
            register their IDs with set_current_job() to make get_current_job() calls work properly
            inside 'func'.
        """
        next(counter)
        if current_job:
            set_current_job(current_job)

        try:
          ret = func(*args)
        except Exception as exc:
          trace = traceback.format_exc()
          log.error("Error in subpool: %s \n%s" % (exc, trace))
          raise

        if current_job:
            set_current_job(None)
        return ret

    def inner_iterable():
        """ This will be called inside the pool's main greenlet, which ID also needs to be registered """
        if current_job:
            set_current_job(current_job)

        for x in iterable:
            yield x

        if current_job:
            set_current_job(None)

    start_time = time.time()
    pool = gevent.pool.Pool(size=pool_size)
    ret = pool.map(inner_func, inner_iterable())
    pool.join(raise_error=True)
    total_time = time.time() - start_time

    log.debug("SubPool ran %s greenlets in %0.6fs" % (counter, total_time))

    return ret


def subpool_imap(pool_size, func, iterable, flatten=False, unordered=False, buffer_size=None):
  """ Generator version of subpool_map. Should be used with unordered=True for optimal performance """

  if not pool_size:
    for args in iterable:
      yield func(*args)

  counter = itertools_count()

  current_job = get_current_job()

  def inner_func(*args):
    """ As each call to 'func' will be done in a random greenlet of the subpool, we need to
        register their IDs with set_current_job() to make get_current_job() calls work properly
        inside 'func'.
    """
    next(counter)
    if current_job:
      set_current_job(current_job)

    try:
      ret = func(*args)
    except Exception as exc:
      trace = traceback.format_exc()
      log.error("Error in subpool: %s \n%s" % (exc, trace))
      raise

    if current_job:
      set_current_job(None)
    return ret

  def inner_iterable():
    """ This will be called inside the pool's main greenlet, which ID also needs to be registered """
    if current_job:
      set_current_job(current_job)

    for x in iterable:
      yield x

    if current_job:
      set_current_job(None)

  start_time = time.time()
  pool = gevent.pool.Pool(size=pool_size)

  if unordered:
    iterator = pool.imap_unordered(inner_func, inner_iterable(), maxsize=buffer_size or pool_size)
  else:
    iterator = pool.imap(inner_func, inner_iterable())

  for x in iterator:
    if flatten:
      for y in x:
        yield y
    else:
      yield x

  pool.join(raise_error=True)
  total_time = time.time() - start_time

  log.debug("SubPool ran %s greenlets in %0.6fs" % (counter, total_time))


def run_task(path, params):
    """ Runs a task code synchronously """
    task_class = load_class_by_path(path)
    return task_class().run_wrapped(params)


def set_current_job_progress(ratio, save=False):
    job = get_current_job()
    if job:
        job.set_progress(ratio, save=save)


# Imports for backward compatibility
def queue_raw_jobs(*args, **kwargs):
    from . import job
    return job.queue_raw_jobs(*args, **kwargs)


def queue_job(*args, **kwargs):
    from . import job
    return job.queue_job(*args, **kwargs)


def queue_jobs(*args, **kwargs):
    from . import job
    return job.queue_jobs(*args, **kwargs)


def metric(*args, **kwargs):
    from . import helpers
    return helpers.metric(*args, **kwargs)
