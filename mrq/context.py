from future import standard_library
standard_library.install_aliases()
from future.builtins import next, map
from past.builtins import basestring
import logging
import gevent
import gevent.pool
import urllib.parse
import sys
import time
import pymongo
import traceback
from .utils import LazyObject, load_class_by_path
from .config import get_config
from .subpool import subpool_map, subpool_imap

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
log = logging.getLogger("mrq.current")


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
        _GLOBAL_CONTEXT["greenlets"][id(current)] = (current, job)


def get_current_job(greenlet_id=None):
    if greenlet_id is None:
        greenlet_id = id(gevent.getcurrent())
    pair = _GLOBAL_CONTEXT["greenlets"].get(greenlet_id)
    if not pair:
        return None
    return pair[1]


def set_current_worker(worker):
    _GLOBAL_CONTEXT["worker"] = worker


def get_current_worker():
    return _GLOBAL_CONTEXT["worker"]

def set_logger_config():
    config = _GLOBAL_CONTEXT["config"]
    if config.get("quiet"):
        log.disabled = True
    else:
        log_format = config["log_format"]
        logging.basicConfig(format=log_format)
        log.setLevel(getattr(logging, config["log_level"]))

        handlers = config["log_handlers"].keys() if config["log_handlers"] else [config["log_handler"]]
        for handler in handlers:
            handler_class = load_class_by_path(handler)
            handler_config = config["log_handlers"].get(handler, {})
            handler_format = handler_config.pop("format", log_format)
            handler_level = getattr(logging, handler_config.pop("level", config["log_level"]))
            log_handler = handler_class(**handler_config)
            formatter = logging.Formatter(handler_format)
            log_handler.setFormatter(formatter)
            log_handler.setLevel(handler_level)
            log.addHandler(log_handler)


def set_current_config(config):
    _GLOBAL_CONTEXT["config"] = config

    if config["add_network_latency"] != "0" and config["add_network_latency"]:
        from mrq.monkey import patch_network_latency
        patch_network_latency(config["add_network_latency"])

    if config["print_mongodb"] or config["trace_io"]:
        from mrq.monkey import patch_pymongo
        patch_pymongo(config)

    if config["trace_io"]:
        from mrq.monkey import patch_io_all
        patch_io_all(config)


def get_current_config():
    if not _GLOBAL_CONTEXT["config"]:
        log.warning("get_current_config was called before setup of MRQ's environment. "
                    "Use context.setup_context() for setting up MRQ's environment.")
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
                decode_responses=False
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
