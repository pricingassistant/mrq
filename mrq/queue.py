from __future__ import division

from builtins import range
from builtins import object
from past.utils import old_div
from .redishelpers import redis_zaddbyscore, redis_zpopbyscore, redis_lpopsafe
from .redishelpers import redis_group_command
import time
from bson import ObjectId
from . import context
from . import job as jobmodule
import binascii

import sys
PY3 = sys.version_info > (3,)
from builtins import bytes
from future import standard_library
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

    use_large_ids = False

    # This is a mutable type so it is shared by all instances
    # of Queue in the current process
    known_queues = {}
    paused_queues = set()

    def __init__(self, queue_id, add_to_known_queues=False):

        if isinstance(queue_id, Queue):
            self.id = queue_id.id  # TODO use __new__?
            self.is_reverse = queue_id.is_reverse
        else:
            if queue_id[-8:] == "_reverse":
                self.is_reverse = True
                queue_id = queue_id[:-8]
            self.id = queue_id

        # Queue types are determined by their suffix.
        if "_raw" in self.id:
            self.is_raw = True

        if "_set" in self.id:
            self.is_set = True
            self.is_raw = True

        if "_timed" in self.id:
            self.is_timed = True
            self.is_sorted = True

        if "_sorted" in self.id:
            self.is_sorted = True

        self.root_id = self.id

        delimiter = context.get_current_config().get("subqueues_delimiter")
        if delimiter is not None and delimiter in self.id:
            # Get the root queue id with no trailing delimiter
            self.root_id = self.id.split(delimiter)[0]

        self.use_large_ids = context.get_current_config()["use_large_job_ids"]

        # If this is the first time this process sees this queue, try to add it
        # on the shared redis set.
        if add_to_known_queues and self.id not in self.known_queues:
            self.add_to_known_queues()

    @property
    def redis_key(self):
        """ Returns the redis key used to store this queue. """
        return "%s:q:%s" % (context.get_current_config()["redis_prefix"], self.id)

    @classmethod
    def redis_key_started(cls):
        """ Returns the global redis key used to store started job ids """
        return "%s:s:started" % context.get_current_config()["redis_prefix"]

    @classmethod
    def redis_key_paused_queues(cls):
        """ Returns the redis key used to store this queue. """
        return "%s:s:paused" % (context.get_current_config()["redis_prefix"])

    @classmethod
    def redis_key_known_queues(cls):
        """ Returns the global redis key used to store started job ids """
        return "%s:known_queues_zset" % context.get_current_config()["redis_prefix"]

    def get_retry_queue(self):
        """ For raw queues, returns the name of the linked queue for job statuses
            other than "queued" """

        if not self.is_raw:
            return self.id

        return self.get_config().get("retry_queue") or "default"

    def add_to_known_queues(self, timestamp=None):
        """ Adds this queue to the shared list of known queues """
        now = timestamp or time.time()
        context.connections.redis.zadd(Queue.redis_key_known_queues(), now, self.id)
        self.known_queues[self.id] = now

    def remove_from_known_queues(self):
        """ Removes this queue from the shared list of known queues """
        context.connections.redis.zrem(Queue.redis_key_known_queues(), self.id)
        self.known_queues.pop(self.id, None)

    @classmethod
    def redis_known_queues(cls):
        """
            Returns the global known_queues as stored in redis as a {name: time_last_used} dict
            with an accuracy of ~1 day
        """
        return {
            value: int(score)
            for value, score in context.connections.redis.zrange(cls.redis_key_known_queues(), 0, -1, withscores=True)
        }

    @classmethod
    def redis_paused_queues(cls):
        """ Returns the set of currently paused queues """
        return context.connections.redis.smembers(cls.redis_key_paused_queues())

    def redis_known_subqueues(self):
        """ Return the known subqueues of this queue as Queue objects. """
        delimiter = context.get_current_config()["subqueues_delimiter"]
        queues = []

        if not self.id.endswith(delimiter):
            return queues

        for key in Queue.known_queues:
            if key.startswith(self.id) and not key.endswith(delimiter):
                queues.append(Queue(key, add_to_known_queues=True))

        return queues

    def get_config(self):
        """ Returns the specific configuration for this queue """

        return context.get_current_config().get("raw_queues", {}).get(self.root_id) or {}

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
      delimiter = context.get_current_config().get("subqueues_delimiter")
      if delimiter is not None and self.id.endswith(delimiter):
          queue = self.root_id
      return queue

    def pause(self):
        """ Adds this queue to the set of paused queues """
        context.connections.redis.sadd(Queue.redis_key_paused_queues(), self._get_pausable_id())

    def is_paused(self):
        """
            Returns wether the queue is paused or not.
            Warning: this does NOT ensure that the queue was effectively added to
            the set of paused queues. See the 'paused_queues_refresh_interval' option.
        """
        root_is_paused = False
        if self.root_id != self.id:
            root_is_paused = context.connections.redis.sismember(Queue.redis_key_paused_queues(), self.root_id)

        return root_is_paused or context.connections.redis.sismember(Queue.redis_key_paused_queues(), self.id)

    def resume(self):
        """ Resumes a paused queue """
        context.connections.redis.srem(Queue.redis_key_paused_queues(), self._get_pausable_id())

    def size(self):
        """ Returns the total number of jobs on the queue """

        # ZSET
        if self.is_sorted:
            return context.connections.redis.zcard(self.redis_key)
        # SET
        elif self.is_set:
            return context.connections.redis.scard(self.redis_key)
        # LIST
        else:
            return context.connections.redis.llen(self.redis_key)

    def count_jobs_to_dequeue(self):
        """ Returns the number of jobs that can be dequeued right now from the queue. """

        # timed ZSET
        if self.is_timed:
            return context.connections.redis.zcount(
                self.redis_key,
                "-inf",
                time.time())

        # In all other cases, it's the same as .size()
        else:
            return self.size()

    def list_job_ids(self, skip=0, limit=20):
        """ Returns a list of job ids on a queue """

        if self.is_raw:
            raise Exception("Can't list job ids from a raw queue")

        return self.unserialize_job_ids(self._get_queue_content(skip, limit))

    def list_raw_jobs(self, skip=0, limit=20):

        if not self.is_raw:
            raise Exception("Queue is not raw")

        return self._get_queue_content(skip, limit)

    def _get_queue_content(self, skip, limit):
        if self.is_sorted:
            return context.connections.redis.zrange(
                self.redis_key,
                skip,
                skip + limit - 1)
        # SET
        elif self.is_set:
            return context.connections.redis.srandmember(self.redis_key, limit)

        # LIST
        else:
            return context.connections.redis.lrange(
                self.redis_key,
                skip,
                skip + limit - 1)

    def get_sorted_graph(
            self,
            start=0,
            stop=100,
            slices=100,
            include_inf=False,
            exact=False):
        """ Returns a graph of the distribution of jobs in a sorted set """

        if not self.is_sorted:
            raise Exception("Not a sorted queue")

        with context.connections.redis.pipeline(transaction=exact) as pipe:
            interval = old_div(float(stop - start), slices)
            for i in range(0, slices):
                pipe.zcount(self.redis_key,
                            (start + i * interval),
                            "(%s" % (start + (i + 1) * interval))
            if include_inf:
                pipe.zcount(self.redis_key, stop, "+inf")
                pipe.zcount(self.redis_key, "-inf", "(%s" % start)
            data = pipe.execute()

        if include_inf:
            return data[-1:] + data[:-1]

        return data

    @classmethod
    def all_active(cls):
        """ List active queues, based on their lengths in Redis. """

        prefix = context.get_current_config()["redis_prefix"]
        queues = []
        for key in context.connections.redis:
            if key.startswith(prefix):
                queues.append(Queue(key[len(prefix) + 3:]))

        return queues

    @classmethod
    def all_known(cls):
        """ List all previously known queues """

        # queues we know exist from the config + known queues in redis
        return cls.all_known_from_config().union(set(cls.redis_known_queues().keys()))

    @classmethod
    def all_known_from_config(cls):
        """ List all known queues from config (raw and regular). Caution: this does not account for the
            configuration of workers' queues (usually given via command line)
        """

        cfg = context.get_current_config()

        queues_from_config = [
          t.get("queue")
          for t in (cfg.get("tasks") or {}).values()
          if t.get("queue")
        ]

        queues_from_config += (cfg.get("raw_queues") or {}).keys()

        queues_from_config += [
          t.get("retry_queue")
          for t in (cfg.get("raw_queues") or {}).values()
          if t.get("retry_queue")
        ]

        return set(queues_from_config)

    @classmethod
    def all(cls):
        """ List *all* queues in MongoDB via aggregation. Might be slow. """

        # Start with raw queues we know exist from the config
        queues = {x: 0 for x in context.get_current_config().get("raw_queues", {})}

        stats = list(context.connections.mongodb_jobs.mrq_jobs.aggregate([
            {"$match": {"status": "queued"}},
            {"$group": {"_id": "$queue", "jobs": {"$sum": 1}}}
        ]))

        queues.update({x["_id"]: x["jobs"] for x in stats})

        return queues

    def enqueue_job_ids(self, job_ids):
        """ Add Jobs to this queue, once they have been inserted in MongoDB. """

        if len(job_ids) == 0:
            return

        if self.is_raw:
            raise Exception("Can't queue regular jobs on a raw queue")

        # ZSET
        if self.is_sorted:

            if not isinstance(job_ids, dict) and self.is_timed:
                now = time.time()
                job_ids = {x: now for x in self.serialize_job_ids(job_ids)}
            else:

                serialized_job_ids = self.serialize_job_ids(list(job_ids.keys()))
                values = list(job_ids.values())
                job_ids = {k: values[i] for i, k in enumerate(serialized_job_ids)}

            context.connections.redis.zadd(self.redis_key, **job_ids)

        # LIST
        else:
            context.connections.redis.rpush(self.redis_key, *self.serialize_job_ids(job_ids))

        context.metric("queues.%s.enqueued" % self.id, len(job_ids))
        context.metric("queues.all.enqueued", len(job_ids))

        # Update the timestamp of the queue in the known queues if it's older than 1 day
        if self.id not in self.known_queues or self.known_queues[self.id] < time.time() - 86400:
            self.add_to_known_queues()

    def enqueue_raw_jobs(self, params_list):
        """ Add Jobs to this queue with raw parameters. They are not yet in MongoDB. """

        if not self.is_raw:
            raise Exception("Can't queue raw jobs in a regular queue")

        if len(params_list) == 0:
            return

        # ZSET
        if self.is_sorted:

            if not isinstance(params_list, dict) and self.is_timed:
                now = time.time()
                params_list = {x: now for x in params_list}

            context.connections.redis.zadd(self.redis_key, **params_list)

        # SET
        elif self.is_set:
            context.connections.redis.sadd(self.redis_key, *params_list)

        # LIST
        else:
            context.connections.redis.rpush(self.redis_key, *params_list)

        context.metric("queues.%s.enqueued" % self.id, len(params_list))
        context.metric("queues.all.enqueued", len(params_list))

        # Update the timestamp of the queue in the known queues if it's older than 1 day
        if self.id not in self.known_queues or self.known_queues[self.id] < time.time() - 86400:
            self.add_to_known_queues()

    def remove_raw_jobs(self, params_list):
        """ Remove jobs from a raw queue with their raw params. """

        if not self.is_raw:
            raise Exception("Can't remove raw jobs in a regular queue")

        if len(params_list) == 0:
            return

        # ZSET
        if self.is_sorted:
            context.connections.redis.zrem(self.redis_key, *iter(params_list))

        # SET
        elif self.is_set:
            context.connections.redis.srem(self.redis_key, *params_list)

        else:
            # O(n)! Use with caution.
            for k in params_list:
                context.connections.redis.lrem(self.redis_key, 1, k)

        context.metric("queues.%s.removed" % self.id, len(params_list))
        context.metric("queues.all.removed", len(params_list))

    def empty(self):
        """ Empty a queue. """
        self.remove_from_known_queues()
        return context.connections.redis.delete(self.redis_key)

    def dequeue_jobs(self, max_jobs=1, job_class=None, worker=None):
        """ Fetch a maximum of max_jobs from this queue """

        if job_class is None:
            from .job import Job
            job_class = Job

        # Used in tests to simulate workers exiting abruptly
        simulate_zombie_jobs = context.get_current_config().get("simulate_zombie_jobs")

        jobs = []

        if self.is_raw:

            queue_config = self.get_config()

            statuses_no_storage = queue_config.get("statuses_no_storage")
            job_factory = queue_config.get("job_factory")
            if not job_factory:
                raise Exception("No job_factory configured for raw queue %s" % self.id)

            retry_queue = self.get_retry_queue()

            params = []

            # ZSET with times
            if self.is_timed:

                current_time = time.time()

                # When we have a pushback_seconds argument, we never pop items from
                # the queue, instead we push them back by an amount of time so
                # that they don't get dequeued again until
                # the task finishes.

                pushback_time = current_time + float(queue_config.get("pushback_seconds") or 0)
                if pushback_time > current_time:
                    params = redis_zaddbyscore()(
                        keys=[self.redis_key],
                        args=[
                            "-inf", current_time, 0, max_jobs, pushback_time
                        ])

                else:
                    params = redis_zpopbyscore()(
                        keys=[self.redis_key],
                        args=[
                            "-inf", current_time, 0, max_jobs
                        ])

            # ZSET
            elif self.is_sorted:

                # TODO Lua?
                with context.connections.redis.pipeline(transaction=True) as pipe:
                    pipe.zrange(self.redis_key, 0, max_jobs - 1)
                    pipe.zremrangebyrank(self.redis_key, 0, max_jobs - 1)
                    params = pipe.execute()[0]

            # SET
            elif self.is_set:
                params = redis_group_command("spop", max_jobs, self.redis_key)

            # LIST
            else:
                params = redis_group_command("lpop", max_jobs, self.redis_key)

            if len(params) == 0:
                return []

            # Caution, not having a pushback_time may result in lost jobs if the worker interrupts
            # before the mongo insert!
            if simulate_zombie_jobs:
                return []

            if worker:
                worker.status = "spawn"

            job_data = [job_factory(p) for p in params]
            for j in job_data:
                j["status"] = "started"
                j["queue"] = retry_queue
                j["raw_queue"] = self.id
                if worker:
                    j["worker"] = worker.id

            jobs += job_class.insert(job_data, statuses_no_storage=statuses_no_storage)

        # Regular queue, in a LIST
        else:

            # TODO implement _timed and _sorted queues here.

            job_ids = redis_lpopsafe()(keys=[
                self.redis_key,
                Queue.redis_key_started()
            ], args=[
                max_jobs,
                time.time(),
                "0" if self.is_reverse else "1"
            ])

            if len(job_ids) == 0:
                return []

            # At this point, the job is in the redis started zset but not in Mongo yet.
            # It may become "zombie" if we interrupt here but we can recover it from
            # the started zset.
            if simulate_zombie_jobs:
                return []

            if worker:
                worker.status = "spawn"
                worker.idle_event.clear()

            jobs += [job_class(_job_id, queue=self.id, start=True)
                     for _job_id in self.unserialize_job_ids(job_ids) if _job_id]

            # Now that the jobs have been marked as started in Mongo, we can
            # remove them from the started queue.
            context.connections.redis.zrem(Queue.redis_key_started(), *job_ids)

        for job in jobs:
            context.metric("queues.%s.dequeued" % job.queue, 1)
        context.metric("queues.all.dequeued", len(jobs))

        return jobs


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
