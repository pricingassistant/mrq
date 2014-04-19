from .utils import load_class_by_path, group_iter, memoize
from .context import connections, get_current_config, log
import time


@memoize
def _redis_command_zaddbyscore():
  """ Increments multiple keys in a sorted set & returns them """

  return connections.redis.register_script("""
local zset = KEYS[1]
local min = ARGV[1]
local max = ARGV[2]
local offset = ARGV[3]
local count = ARGV[4]
local score = ARGV[5]

local data = redis.call('zrangebyscore', zset, min, max, 'LIMIT', offset, count)
for i, member in pairs(data) do
  redis.call('zadd', zset, score, member)
end

return data
  """)


@memoize
def _redis_command_zpopbyscore():
  """ Pops multiple keys by score """

  return connections.redis.register_script("""
local zset = KEYS[1]
local min = ARGV[1]
local max = ARGV[2]
local offset = ARGV[3]
local count = ARGV[4]

local data = redis.call('zrangebyscore', zset, min, max, 'LIMIT', offset, count)
if #data > 0 then
  redis.call('zremrangebyrank', zset, 0, #data - 1)
end

return data
  """)


class Queue(object):

  is_raw = False
  is_timed = False
  is_sorted = False
  is_set = False

  def __init__(self, queue_id):
    if isinstance(queue_id, Queue):
      self.id = queue_id.id  # TODO use __new__?
    else:
      self.id = queue_id

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
    return "%s:q:%s" % (get_current_config()["redis_prefix"], self.id)

  def enqueue_job_ids(self, job_ids):

    if self.is_raw:

      retry_queue = get_current_config().get("raw_queues", {}).get(self.id, {}).get("retry_queue", "default")

      queue = Queue(retry_queue)
    else:
      queue = self

    # ZSET
    if queue.is_sorted:

      if type(job_ids) is not dict and queue.is_timed:
        now = int(time.time())
        job_ids = {x: now for x in job_ids}

      connections.redis.zadd(queue.redis_key, **job_ids)

    # LIST
    else:
      connections.redis.rpush(queue.redis_key, *job_ids)

  def enqueue_raw_jobs(self, params_list):

    if not self.is_raw:
      raise Exception("Can't queue raw jobs in a regular queue")

    # ZSET
    if self.is_sorted:

      if type(params_list) is not dict and self.is_timed:
        now = int(time.time())
        params_list = {x: now for x in params_list}

      connections.redis.zadd(self.redis_key, **params_list)
    # SET
    elif self.is_set:
      connections.redis.sadd(self.redis_key, *params_list)
    # LIST
    else:
      connections.redis.rpush(self.redis_key, *params_list)

  def size(self):

    # ZSET
    if self.is_sorted:
      return connections.redis.zcard(self.redis_key)
    # SET
    elif self.is_set:
      return connections.redis.scard(self.redis_key)
    # LIST
    else:
      return connections.redis.llen(self.redis_key)

  def empty(self):
    return connections.redis.delete(self.redis_key)

  def list_job_ids(self, skip=0, limit=20):

    if self.is_raw:
      raise Exception("Can't list job ids from a raw queue")

    # ZSET
    if self.is_sorted:
      return connections.redis.zrange(self.redis_key, skip, skip + limit - 1)
    # SET
    elif self.is_set:
      return connections.redis.srandmember(self.redis_key, limit)
    # LIST
    else:
      return connections.redis.lrange(self.redis_key, skip, skip + limit - 1)

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
    """ List all queues, based on their MongoDB existence. """

    stats = list(connections.mongodb_jobs.mrq_jobs.aggregate([
      {"$match": {"status": "queued"}},
      {"$group": {"_id": "$queue", "jobs": {"$sum": 1}}}
    ])["result"])

    return {x["_id"]: x["jobs"] for x in stats}

  @classmethod
  def dequeue_jobs(self, queues, max_jobs=1, job_class=None):
    """ Fetch a maximum of max_jobs from this worker's queues. """

    if job_class is None:
      from .job import Job
      job_class = Job

    queue_objects = [Queue(q) for q in queues]

    # TODO: we may allow dequeueing from several raw queues in the future
    has_raw = any(q.is_raw or q.is_timed for q in queue_objects)
    if has_raw and len(queues) != 1:
      raise Exception("Can't dequeue from more multiple queues when one is raw or timed")

    log.debug("Fetching %s jobs from Redis" % max_jobs)

    if not has_raw:

      jobs = []

      if queue_objects[0].is_timed:
        queue = queue_objects[0]
        with connections.redis.pipeline(transaction=True) as pipe:
          pipe.zrange(queue.redis_key, 0, max_jobs)
          pipe.zremrangebyrank(queue.redis_key, 0, max_jobs)
          job_ids = pipe.execute()

        # From this point until job.fetch_and_start(), job is only local to this worker.
        # If we die here, job will be lost in redis without having been marked as "started".

        self.status = "spawn"
        jobs = [job_class(_job_id, queue=queue.redis_key, start=True) for _job_id in job_ids]

      else:

        queue, job_id = connections.redis.blpop([q.redis_key for q in queue_objects], 0)
        self.status = "spawn"

        # From this point until job.fetch_and_start(), job is only local to this worker.
        # If we die here, job will be lost in redis without having been marked as "started".

        jobs.append(job_class(job_id, queue=queue, start=True))

        # Bulk-fetch other jobs from that queue to fill the pool.
        # We take the chance that if there was one job on that queue, there should be more.
        if max_jobs > 1:
          job_ids = _redis_group_command("lpop", max_jobs - 1, queue)

          jobs += [job_class(_job_id, queue=queue, start=True)
                   for _job_id in job_ids if _job_id]

      return jobs

    else:
      queue = queue_objects[0]

      queue_config = get_current_config().get("raw_queues", {}).get(queue.id, {})

      job_factory = queue_config.get("job_factory")
      if not job_factory:
        raise Exception("No job_factory configured for queue %s" % queue.id)

      params = []

      # ZSET with times
      if queue.is_timed:

        current_time = int(time.time())

        # When we have a pushback_seconds argument, we never pop items from the queue, instead
        # we push them back by an amount of time so that they don't get dequeued again until
        # the task finishes.
        if queue_config.get("pushback_seconds"):
          pushback_time = current_time + queue_config.get("pushback_seconds")
          params = _redis_command_zaddbyscore()(keys=[queue.redis_key], args=["-inf", current_time, 0, max_jobs, pushback_time])
        else:
          params = _redis_command_zpopbyscore()(keys=[queue.redis_key], args=["-inf", current_time, 0, max_jobs])

      # ZSET
      elif queue.is_sorted:

        with connections.redis.pipeline(transaction=True) as pipe:
          pipe.zrange(queue.redis_key, 0, max_jobs - 1)
          pipe.zremrangebyrank(queue.redis_key, 0, max_jobs - 1)
          params = pipe.execute()[0]

      # SET
      elif queue.is_set:
        params = _redis_group_command("spop", max_jobs, queue.redis_key)

      # LIST
      else:
        params = _redis_group_command("lpop", max_jobs, queue.redis_key)

      if len(params) == 0:
        return []

      job_data = [job_factory(p) for p in params]
      for j in job_data:
        j["status"] = "started"
        j["queue"] = queue.id

      from .job import Job
      return [Job.insert(j) for j in job_data]  # TODO could be optimized by bulk inserts


def _redis_group_command(command, cnt, redis_key):
  with connections.redis.pipeline(transaction=False) as pipe:
    for _ in range(cnt):
      getattr(pipe, command)(redis_key)
    return [x for x in pipe.execute() if x]


def send_raw_tasks(queue, params_list, **kwargs):
  q = Queue(queue)
  q.enqueue_raw_jobs(params_list, **kwargs)


def send_task(path, params, **kwargs):
  return send_tasks(path, [params], **kwargs)[0]


def send_tasks(path, params_list, queue=None, sync=False, batch_size=1000):

  if len(params_list) == 0:
    return []

  if sync:
    task_class = load_class_by_path(path)
    return [task_class().run(params) for params in params_list]

  if queue is None:
    task_def = get_current_config().get("tasks", {}).get(path) or {}
    queue = task_def.get("queue", "default")

  all_ids = []

  collection = connections.mongodb_jobs.mrq_jobs

  for params_group in group_iter(params_list, n=batch_size):

    job_ids = collection.insert([{
      "path": path,
      "params": params,
      "queue": queue,
      "status": "queued"
    } for params in params_group], w=1)

    # Between these 2 calls, a task can be inserted in MongoDB but not queued in Redis.
    # This is the same as dequeueing a task from Redis and being stopped before updating the "started"
    # flag in MongoDB.

    Queue(queue).enqueue_job_ids([str(x) for x in job_ids])

    all_ids += job_ids

  return all_ids
