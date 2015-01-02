from .logger import Logger
import gevent
import gevent.pool
import urlparse
import re
import time
from .utils import LazyObject, load_class_by_path, group_iter
from itertools import count as itertools_count

_GLOBAL_CONTEXT = {

    # Contains all the running greenlets for this worker. greenletid => Job object
    "greenlets": {},

    # pointer to the current worker
    "worker": None,

    # pointer to the current config
    "config": None
}

# Global log object, usable from all tasks
log = Logger(None, job="current")


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
    set_current_config(worker.config)


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
        patch_io_all()

    if not config["no_import_patch"]:
        from mrq.monkey import patch_import
        patch_import()


def get_current_config():
    return _GLOBAL_CONTEXT["config"]


def retry_current_job(delay=None, max_retries=None, queue=None):
    current_job = get_current_job()
    if current_job:
        current_job.retry(delay=delay, max_retries=max_retries, queue=queue)


def _connections_factory(attr):

    config = get_current_config()

    # Connection strings may be stored directly in config
    config_obj = config.get(attr)

    if attr.startswith("redis"):
        if type(config_obj) in [str, unicode]:

            import redis as pyredis

            urlparse.uses_netloc.append('redis')
            redis_url = urlparse.urlparse(config_obj)

            log.info("%s: Connecting to Redis at %s..." %
                     (attr, redis_url.hostname))

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

            try:
                # MongoKit's Connection object is just a wrapped MongoClient.
                from mongokit import Connection as MongoClient   # pylint: disable=import-error
                from mongokit import ReplicaSetConnection as MongoReplicaSetClient  # pylint: disable=import-error
            except ImportError:
                from pymongo import MongoClient
                from pymongo import MongoReplicaSetClient

            mongo_parsed = re.match(
                r"mongodb://((\w+):(\w+)@)?([\w\.:,-]+)/([\w-]+)(\?.*)?",
                config_obj
            ).groups()

            mongo_hosts = mongo_parsed[3]
            mongo_name = mongo_parsed[4]
            mongo_options = mongo_parsed[5]

            log.debug("%s: Connecting to MongoDB at %s/%s..." % (attr, mongo_hosts, mongo_name))

            kwargs = {"use_greenlets": True}
            options = {}
            if mongo_options:
                options = {
                    k: v[0]
                    for k, v in urlparse.parse_qs(mongo_options[1:]).iteritems()
                }

            # We automatically switch to MongoReplicaSetClient when getting a replicaSet option.
            # This should cover most use-cases.
            # http://api.mongodb.org/python/current/examples/high_availability.html#mongoreplicasetclient
            if options.get("replicaSet"):
                db = MongoReplicaSetClient(config_obj, **kwargs)[mongo_name]
            else:
                db = MongoClient(config_obj, **kwargs)[mongo_name]
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


def set_job_progress(ratio, save=False):
    job = get_current_job()
    if job:
        job.set_progress(ratio, save=save)


def metric(name, incr=1, **kwargs):
    cfg = get_current_config()
    if cfg.get("metric_hook"):
        return cfg.get("metric_hook")(name, incr=incr, **kwargs)


def subpool_map(pool_size, func, iterable):
    """ Starts a Gevent pool and run a map. Takes care of setting current_job and cleaning up. """

    if not pool_size:
        return [func(*args) for args in iterable]

    counter = itertools_count()

    current_job = get_current_job()

    def inner_func(*args):
        next(counter)
        if current_job:
            set_current_job(current_job)
        ret = func(*args)
        if current_job:
            set_current_job(None)
        return ret

    start_time = time.time()
    pool = gevent.pool.Pool(size=pool_size)
    ret = pool.map(inner_func, iterable)
    pool.join(raise_error=True)
    total_time = time.time() - start_time

    log.debug("SubPool ran %s greenlets in %0.6fs" % (counter, total_time))

    return ret


def run_task(path, params):
    """ Runs a task code synchronously """
    task_class = load_class_by_path(path)
    return task_class().run_wrapped(params)


def queue_raw_jobs(queue, params_list, **kwargs):
    """ Queue some jobs on a raw queue """

    from .queue import Queue
    queue_obj = Queue(queue)
    queue_obj.enqueue_raw_jobs(params_list, **kwargs)


def queue_job(main_task_path, params, **kwargs):
    """ Queue one job on a regular queue """

    return queue_jobs(main_task_path, [params], **kwargs)[0]


def queue_jobs(main_task_path, params_list, queue=None, batch_size=1000):
    """ Queue multiple jobs on a regular queue """

    if len(params_list) == 0:
        return []

    if queue is None:
        task_def = get_current_config().get("tasks", {}).get(main_task_path) or {}
        queue = task_def.get("queue", "default")

    from .queue import Queue
    from .job import Job

    queue_obj = Queue(queue)

    if queue_obj.is_raw:
        raise Exception("Can't queue regular jobs on a raw queue")

    all_ids = []

    for params_group in group_iter(params_list, n=batch_size):

        metric("jobs.status.queued", len(params_group))

        # Insert the job in MongoDB
        job_ids = Job.insert([{
            "path": main_task_path,
            "params": params,
            "queue": queue,
            "status": "queued"
        } for params in params_group], w=1, return_jobs=False)

        # Between these 2 calls, a task can be inserted in MongoDB but not queued in Redis.
        # This is the same as dequeueing a task from Redis and being stopped before updating
        # the "started" flag in MongoDB.
        #
        # These jobs will be collected by mrq.basetasks.cleaning.RequeueLostJobs

        # Insert the job ID in Redis
        queue_obj.enqueue_job_ids([str(x) for x in job_ids])

        all_ids += job_ids

    return all_ids
