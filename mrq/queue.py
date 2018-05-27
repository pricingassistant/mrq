from __future__ import division

from future.builtins import bytes, str, object

import time
from bson import ObjectId
from . import context
from . import job as jobmodule
import binascii
from .redishelpers import redis_key

import sys
from future import standard_library
from itertools import chain

PY3 = sys.version_info > (3,)
standard_library.install_aliases()


class Queue(object):
    """ A Queue for Jobs. """

    is_raw = False
    is_timed = False
    is_sorted = False
    is_set = False

    is_reverse = False

    # root_id will contain the root queue id without any trailing subqueue delimiter
    # e.g. if self.id is "some_queue/" then self.root_id will contain "some_queue"
    # and if self.id is "some_queue/some_subqueue" then self.root_id will contain "some_queue"
    root_id = None
    is_subqueue = False

    use_large_ids = False

    # This is a mutable type so it is shared by all instances
    # of Queue in the current process
    paused_queues = set()

    def __new__(cls, queue_id, **kwargs):
        """ Creates a new instance of the right queue type """

        if cls is not Queue:
            return object.__new__(cls)

        if isinstance(queue_id, Queue):
            queue_id = queue_id.id

        queue_type = Queue.get_queue_type(queue_id)

        if queue_type == "regular":
            from .queue_regular import QueueRegular
            return QueueRegular(queue_id, **kwargs)
        else:
            from .queue_raw import QueueRaw
            return QueueRaw(queue_id, **kwargs)

    def __init__(self, queue_id):

        if queue_id[-8:] == "_reverse":
            self.is_reverse = True
            queue_id = queue_id[:-8]

        self.id = queue_id

        self.root_id = self.id

        if "/" in self.id:
            # Get the root queue id with no trailing delimiter
            self.root_id = self.id.split("/")[0]

            if self.id.split("/")[1]:
                self.is_subqueue = True

        self.use_large_ids = context.get_current_config()["use_large_job_ids"]

    @classmethod
    def get_queue_type(cls, queue_id):
        """ Return the queue type, currently determined only by its suffix. """

        for queue_type in ("timed_set", "sorted_set", "set", "raw"):
            if queue_id.split("/")[0].endswith("_%s" % queue_type):
                return queue_type

        return "regular"

    @classmethod
    def get_queues_config(cls):
        """ Returns the queues configuration dict """
        _config = context.get_current_config()
        return _config.get("raw_queues") or _config.get("queues_config") or {}

    def get_config(self):
        """ Returns the specific configuration for this queue """
        return Queue.get_queues_config().get(self.root_id) or {}

    def serialize_job_ids(self, job_ids):
        """ Returns job_ids serialized for storage in Redis """
        if len(job_ids) == 0 or self.use_large_ids:
            return job_ids
        elif isinstance(job_ids[0], ObjectId):
            return [x.binary for x in job_ids]
        else:
            return [bytes.fromhex(str(x)) for x in job_ids]

    def unserialize_job_ids(self, job_ids):
        """ Unserialize job_ids stored in Redis """
        if len(job_ids) == 0 or self.use_large_ids:
            return job_ids
        else:
            return [binascii.hexlify(x.encode('utf-8') if (PY3 and isinstance(x, str)) else x).decode('ascii')
                    for x in job_ids]

    def _get_pausable_id(self):
      """
          Get the queue id (either id or root_id) that should be used to pause/unpause the current queue
          TODO: handle subqueues with more than one level, e.g. "queue/subqueue/"
      """
      queue = self.id
      if self.id.endswith("/"):
          queue = self.root_id
      return queue

    def pause(self):
        """ Adds this queue to the set of paused queues """
        context.connections.redis.sadd(redis_key("paused_queues"), self._get_pausable_id())

    def is_paused(self):
        """
            Returns wether the queue is paused or not.
            Warning: this does NOT ensure that the queue was effectively added to
            the set of paused queues. See the 'paused_queues_refresh_interval' option.
        """
        root_is_paused = False
        if self.root_id != self.id:
            root_is_paused = context.connections.redis.sismember(redis_key("paused_queues"), self.root_id)

        return root_is_paused or context.connections.redis.sismember(redis_key("paused_queues"), self.id)

    def resume(self):
        """ Resumes a paused queue """
        context.connections.redis.srem(redis_key("paused_queues"), self._get_pausable_id())

    def count_jobs_to_dequeue(self):
        """ Returns the number of jobs that can be dequeued right now from the queue. """

        return self.size()

    @classmethod
    def all_active(cls):
        """ List active queues, based on their lengths in Redis. Warning, uses the unscalable KEYS redis command """

        prefix = context.get_current_config()["redis_prefix"]
        queues = []
        for key in context.connections.redis.keys():
            if key.startswith(prefix):
                queues.append(Queue(key[len(prefix) + 3:]))

        return queues

    @classmethod
    def all_known(cls, sources=None, prefixes=None):
        """ List all currently known queues """

        sources = sources or ("config", "jobs", "raw_subqueues")

        queues = set()

        if "config" in sources and not prefixes:
            # Some queues are explicitly declared in the config (including all root raw queues)
            cfg = context.get_current_config()

            queues_from_config = [
                t.get("queue")
                for t in (cfg.get("tasks") or {}).values()
                if t.get("queue")
            ]

            queues_from_config += Queue.get_queues_config().keys()

            queues_from_config += [
                t.get("retry_queue")
                for t in Queue.get_queues_config().values()
                if t.get("retry_queue")
            ]

            queues |= set(queues_from_config)

        if "jobs" in sources:

            # This will get all queues from mongodb, including those where we have only non-queued jobs
            for q in context.connections.mongodb_jobs.mrq_jobs.distinct("queue"):
                if prefixes and not any(q.startswith(p) for p in prefixes):
                    continue
                queues.add(q)

        if "raw_subqueues" in sources:
            for q in Queue.get_queues_config():
                if prefixes and not any(q + "/" == p for p in prefixes):
                    continue
                queue_obj = Queue(q)
                if queue_obj.is_raw and queue_obj.has_subqueues:
                    # TODO: optimize this with a single SUNION on all keys
                    queues |= queue_obj.get_known_subqueues()

        return queues

    @classmethod
    def all(cls):
        """ List all queues in MongoDB via aggregation, with their queued jobs counts. Might be slow. """

        # Start with raw queues we know exist from the config
        queues = {x: 0 for x in Queue.get_queues_config()}

        stats = list(context.connections.mongodb_jobs.mrq_jobs.aggregate([
            {"$match": {"status": "queued"}},
            {"$group": {"_id": "$queue", "jobs": {"$sum": 1}}}
        ]))

        queues.update({x["_id"]: x["jobs"] for x in stats})

        return queues

    def use_notify(self):
        """ Does this queue use notifications? """
        return bool(self.get_config().get("notify"))

    def notify(self, new_jobs_count):
        """ We just queued new_jobs_count jobs on this queue, wake up the workers if needed """

        if not self.use_notify():
            return

        # Not really useful to send more than 100 notifs (to be configured)
        count = min(new_jobs_count, 100)

        notify_key = redis_key("notify", self)

        context.connections.redis.lpush(notify_key, *([1] * count))
        context.connections.redis.expire(notify_key, max(1, int(context.get_current_config()["max_latency"] * 2)))


#
# Deprecated methods. Tagged for removal in 1.0.0
#

def send_raw_tasks(*args, **kwargs):
    return jobmodule.queue_raw_jobs(*args, **kwargs)


def send_task(path, params, **kwargs):
    return send_tasks(path, [params], **kwargs)[0]


def send_tasks(path, params_list, queue=None, sync=False, batch_size=1000):
    if sync:
        return [context.run_task(path, params) for params in params_list]

    return jobmodule.queue_jobs(path, params_list, queue=queue, batch_size=batch_size)
