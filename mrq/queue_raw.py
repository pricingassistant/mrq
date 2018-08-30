from datetime import datetime
import time
from .queue import Queue
from . import context
from .redishelpers import redis_zaddbyscore, redis_zpopbyscore, redis_key, redis_group_command
from past.utils import old_div
from future.builtins import range


class QueueRaw(Queue):

    is_raw = True

    def __init__(self, queue_id, **kwargs):
        Queue.__init__(self, queue_id, **kwargs)

        queue_type = Queue.get_queue_type(queue_id)
        if "set" in queue_type:
            self.is_set = True
        if "_timed" in self.id:
            self.is_timed = True
            self.is_sorted = True
        elif "_sorted" in self.id:
            self.is_sorted = True

        self.has_subqueues = bool(self.get_config().get("has_subqueues"))

        current_config = context.get_current_config()

        # redis key used to store the known subqueues of this raw queue.
        self.redis_key_known_subqueues = redis_key("known_subqueues", self)

        # redis key used to store this queue.
        self.redis_key = redis_key("queue", self)

        # global redis key used to store started job ids
        self.redis_key_started = redis_key("started_jobs")

    def empty(self):
        """ Empty a queue. """
        with context.connections.redis.pipeline(transaction=True) as pipe:
            pipe.delete(self.redis_key)
            pipe.delete(self.redis_key_known_subqueues)
            pipe.execute()

    def get_known_subqueues(self):
        """ Returns all known subqueues """
        if not self.has_subqueues:
            return set()
        return set(context.connections.redis.smembers(self.redis_key_known_subqueues))

    def size(self):
        """ Returns the total number of queued jobs on the queue """

        if self.id.endswith("/"):
            return sum(Queue(q).size() for q in self.get_known_subqueues())

        # ZSET
        if self.is_sorted:
            return context.connections.redis.zcard(self.redis_key)
        # SET
        elif self.is_set:
            return context.connections.redis.scard(self.redis_key)
        # LIST
        else:
            return context.connections.redis.llen(self.redis_key)

    def enqueue_raw_jobs(self, params_list):
        """ Add Jobs to this queue with raw parameters. They are not yet in MongoDB. """

        if len(params_list) == 0:
            return

        if self.is_subqueue:
            context.connections.redis.sadd(self.redis_key_known_subqueues, self.id)

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

    def remove_raw_jobs(self, params_list):
        """ Remove jobs from a raw queue with their raw params. """

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

    def list_raw_jobs(self, skip=0, limit=20):

        return self._get_queue_content(skip, limit)

    def _get_queue_content(self, skip, limit):

        # ZSET
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

    def get_retry_queue(self):
        """ Return the name of the queue where retried jobs will be queued """

        return self.get_config().get("retry_queue") or "default"

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

    def dequeue_jobs(self, max_jobs=1, job_class=None, worker=None):

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

            # We didn't dequeue anything. Does this mean the queue is empty?
            # TODO LUA this with the above
            if self.is_subqueue and self.size() == 0:
                context.connections.redis.srem(self.redis_key_known_subqueues, self.id)

            return

        if worker:
            worker.status = "spawn"

        job_data = [job_factory(p) for p in params]
        for j in job_data:
            j["status"] = "started"
            j["queue"] = retry_queue
            j["datequeued"] = datetime.now()
            j["raw_queue"] = self.id
            if worker:
                j["worker"] = worker.id
        for job in job_class.insert(job_data, statuses_no_storage=statuses_no_storage):
            yield job

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
