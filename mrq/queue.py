from .utils import load_class_by_path, group_iter
from .context import connections, get_current_config, log, metric
from .redishelpers import redis_zaddbyscore, redis_zpopbyscore, redis_lpopsafe
from .redishelpers import redis_group_command
import time


class Queue(object):

    """ A Queue for Jobs. """

    is_raw = False
    is_timed = False
    is_sorted = False
    is_set = False
    is_reverse = False

    def __init__(self, queue_id):
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

    @property
    def redis_key(self):
        """ Returns the redis key used to store this queue. """
        return "%s:q:%s" % (get_current_config()["redis_prefix"], self.id)

    @classmethod
    def redis_key_started(self):
        """ Returns the global redis key used to store started job ids """
        return "%s:s:started" % get_current_config()["redis_prefix"]

    def get_retry_queue(self):
        """ For raw queues, returns the name of the linked queue for job statuses
            other than "queued" """

        if not self.is_raw:
            return self.id

        return self.get_config().get("retry_queue") or "default"

    def get_config(self):
        """ Returns the specific configuration for this queue """

        return get_current_config().get("raw_queues", {}).get(self.id) or {}

    def size(self):
        """ Returns the total number of jobs on the queue """

        # ZSET
        if self.is_sorted:
            return connections.redis.zcard(self.redis_key)
        # SET
        elif self.is_set:
            return connections.redis.scard(self.redis_key)
        # LIST
        else:
            return connections.redis.llen(self.redis_key)

    def count_jobs_to_dequeue(self):
        """ Returns the number of jobs that can be dequeued right now from the queue. """

        # timed ZSET
        if self.is_timed:
            return connections.redis.zcount(
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

        # ZSET
        if self.is_sorted:
            return connections.redis.zrange(
                self.redis_key,
                skip,
                skip + limit - 1)
        # SET
        elif self.is_set:
            return connections.redis.srandmember(self.redis_key, limit)
        # LIST
        else:
            return connections.redis.lrange(
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

        with connections.redis.pipeline(transaction=exact) as pipe:
            interval = float(stop - start) / slices
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

        prefix = get_current_config()["redis_prefix"]
        queues = []
        for key in connections.redis.keys():
            if key.startswith(prefix):
                queues.append(Queue(key[len(prefix) + 3:]))

        return queues

    @classmethod
    def all(cls):
        """ List all known queues (Raw + MongoDB)"""

        # Start with raw queues we know exist from the config
        queues = {x: 0 for x in get_current_config().get("raw_queues", {})}

        stats = list(connections.mongodb_jobs.mrq_jobs.aggregate([
            {"$match": {"status": "queued"}},
            {"$group": {"_id": "$queue", "jobs": {"$sum": 1}}}
        ])["result"])

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
                job_ids = {str(x): now for x in job_ids}

            connections.redis.zadd(self.redis_key, **job_ids)

        # LIST
        else:
            connections.redis.rpush(self.redis_key, *job_ids)

        metric("queues.%s.enqueued" % self.id, len(job_ids))
        metric("queues.all.enqueued", len(job_ids))

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
                params_list = {str(x): now for x in params_list}

            connections.redis.zadd(self.redis_key, **params_list)

        # SET
        elif self.is_set:
            connections.redis.sadd(self.redis_key, *params_list)

        # LIST
        else:
            connections.redis.rpush(self.redis_key, *params_list)

        metric("queues.%s.enqueued" % self.id, len(params_list))
        metric("queues.all.enqueued", len(params_list))

    def remove_raw_jobs(self, params_list):
        """ Remove jobs from a raw queue with their raw params. """

        if not self.is_raw:
            raise Exception("Can't remove raw jobs in a regular queue")

        if len(params_list) == 0:
            return

        # ZSET
        if self.is_sorted:
            connections.redis.zrem(self.redis_key, *iter(params_list))

        # SET
        elif self.is_set:
            connections.redis.srem(self.redis_key, *params_list)

        else:
            # O(n)! Use with caution.
            for k in params_list:
                connections.redis.lrem(self.redis_key, 1, k)

        metric("queues.%s.removed" % self.id, len(params_list))
        metric("queues.all.removed", len(params_list))

    def empty(self):
        """ Empty a queue. """
        return connections.redis.delete(self.redis_key)

    def dequeue_jobs(self, max_jobs=1, job_class=None, worker=None):
        """ Fetch a maximum of max_jobs from this queue """

        if job_class is None:
            from .job import Job
            job_class = Job

        # Used in tests to simulate workers exiting abruptly
        simulate_zombie_jobs = get_current_config().get("simulate_zombie_jobs")

        jobs = []

        if self.is_raw:

            queue_config = self.get_config()

            job_factory = queue_config.get("job_factory")
            if not job_factory:
                raise Exception("No job_factory configured for raw queue %s" % self.id)

            retry_queue = self.get_retry_queue()

            params = []

            # ZSET with times
            if self.is_timed:

                current_time = time.time()

                # When we have a pushback_seconds argument, we never pop items from the queue, instead
                # we push them back by an amount of time so that they don't get dequeued again until
                # the task finishes.

                # TODO add reverse queue support

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
                with connections.redis.pipeline(transaction=True) as pipe:
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
                if worker:
                    j["worker"] = worker.id

            jobs += job_class.insert(job_data)

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

            jobs += [job_class(_job_id, queue=self.id, start=True)
                     for _job_id in job_ids if _job_id]

            # Now that the jobs have been marked as started in Mongo, we can
            # remove them from the started queue.
            connections.redis.zrem(Queue.redis_key_started(), *job_ids)

        for job in jobs:
            metric("queues.%s.dequeued" % job.queue, 1)
        metric("queues.all.dequeued", len(jobs))

        return jobs


def send_raw_tasks(queue, params_list, **kwargs):
    """ Queue some tasks on a raw queue """

    q = Queue(queue)
    q.enqueue_raw_jobs(params_list, **kwargs)


def send_task(path, params, **kwargs):
    """ Queue one task on a regular queue """

    return send_tasks(path, [params], **kwargs)[0]


def send_tasks(path, params_list, queue=None, sync=False, batch_size=1000):
    """ Queue several task on a regular queue """

    if len(params_list) == 0:
        return []

    if sync:
        task_class = load_class_by_path(path)
        return [task_class().run(params) for params in params_list]

    if queue is None:
        task_def = get_current_config().get("tasks", {}).get(path) or {}
        queue = task_def.get("queue", "default")

    queue_obj = Queue(queue)

    if queue_obj.is_raw:
        raise Exception("Can't queue regular jobs on a raw queue")

    all_ids = []

    # Avoid circular import
    from .job import Job

    for params_group in group_iter(params_list, n=batch_size):

        metric("jobs.status.queued", len(params_group))

        job_ids = Job.insert([{
            "path": path,
            "params": params,
            "queue": queue,
            "status": "queued"
        } for params in params_group], w=1, return_jobs=False)

        # Between these 2 calls, a task can be inserted in MongoDB but not queued in Redis.
        # This is the same as dequeueing a task from Redis and being stopped before updating the "started"
        # flag in MongoDB.

        queue_obj.enqueue_job_ids([str(x) for x in job_ids])

        all_ids += job_ids

    return all_ids
