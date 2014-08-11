from .utils import load_class_by_path, group_iter, memoize
from .context import connections, get_current_config, log, metric
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


@memoize
def _redis_command_lpopsafe():
  """ Increments multiple keys in a sorted set & returns them """

  return connections.redis.register_script("""
local key = KEYS[1]
local zset_started = KEYS[2]
local count = ARGV[1]
local now = ARGV[2]
local left = ARGV[3]
local data = {}
local current = nil

for i=1, count do
  if left == '1' then
    current = redis.call('lpop', key)
  else
    current = redis.call('rpop', key)
  end
  if current == false then
    return data
  end
  data[i] = current
  redis.call('zadd', zset_started, now, current)
end

return data
""")


class Queue(object):

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

  @classmethod
  def redis_key_started(self):
    return "%s:s:started" % get_current_config()["redis_prefix"]

  def enqueue_job_ids(self, job_ids):

    if len(job_ids) == 0:
      return

    if self.is_raw:

      retry_queue = get_current_config().get("raw_queues", {}).get(self.id, {}).get("retry_queue", "default")

      queue = Queue(retry_queue)
    else:
      queue = self

    # ZSET
    if queue.is_sorted:

      if type(job_ids) is not dict and queue.is_timed:
        now = time.time()
        job_ids = {str(x): now for x in job_ids}

      connections.redis.zadd(queue.redis_key, **job_ids)

    # LIST
    else:
      connections.redis.rpush(queue.redis_key, *job_ids)

    metric("queues.%s.enqueued" % self.id, len(job_ids))
    metric("queues.all.enqueued", len(job_ids))

  def enqueue_raw_jobs(self, params_list):

    if not self.is_raw:
      raise Exception("Can't queue raw jobs in a regular queue")

    if len(params_list) == 0:
      return

    # ZSET
    if self.is_sorted:

      if type(params_list) is not dict and self.is_timed:
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

  def count_jobs_to_dequeue(self):
    if self.is_timed:
      # timed ZSET
      return connections.redis.zcount(self.redis_key, "-inf", time.time())
    else:
      return self.size()

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

  def get_sorted_graph(self, start=0, stop=100, slices=100, include_inf=False, exact=False):

    if not self.is_sorted:
      raise Exception("Not a sorted queue")

    with connections.redis.pipeline(transaction=exact) as pipe:
      interval = float(stop - start) / slices
      for i in range(0, slices):
        pipe.zcount(self.redis_key, (start + i * interval), "(%s" % (start + (i + 1) * interval))
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

  @classmethod
  def dequeue_jobs(self, queues, max_jobs=1, job_class=None, worker=None, quiet=False):
    """ Fetch a maximum of max_jobs from this worker's queues. """

    if job_class is None:
      from .job import Job
      job_class = Job

    queue_objects = [Queue(q) for q in queues]

    has_raw = any(q.is_raw or q.is_sorted for q in queue_objects)

    if not quiet:
      log.debug("Fetching %s jobs from Redis" % max_jobs)

    # When none of the queues is a raw queue, we can have an optimized mode where we BLPOP from the queues.
    if not has_raw:

      jobs = []

      if queue_objects[0].is_timed:

        if worker:
          worker.status = "spawn"

        queue = queue_objects[0]
        with connections.redis.pipeline(transaction=True) as pipe:
          pipe.zrange(queue.redis_key, 0, max_jobs)
          pipe.zremrangebyrank(queue.redis_key, 0, max_jobs)
          job_ids = pipe.execute()

        # From this point until job.fetch_and_start(), job is only local to this worker.
        # If we die here, job will be lost in redis without having been marked as "started".

        jobs = [job_class(_job_id, queue=queue.id, start=True) for _job_id in job_ids]

      else:

        simulate_lost_jobs = get_current_config().get("simulate_lost_jobs")

        for queue in queue_objects:

          job_ids = _redis_command_lpopsafe()(keys=[
            queue.redis_key,
            Queue.redis_key_started()
          ], args=[
            max_jobs,
            time.time(),
            "0" if queue.is_reverse else "1"
          ])

          if len(job_ids) == 0:
            continue

          if simulate_lost_jobs:
            break

          if worker:
            worker.status = "spawn"

          jobs += [job_class(_job_id, queue=queue.id, start=True)
                   for _job_id in job_ids if _job_id]

          # Now the jobs have been marked as started in Mongo, we can remove them from the started queue.
          connections.redis.zrem(Queue.redis_key_started(), *job_ids)

          max_jobs -= len(job_ids)

          if max_jobs == 0:
            break

      for job in jobs:
        metric("queues.%s.dequeued" % job.queue, 1)
      metric("queues.all.dequeued", len(jobs))

      return jobs

    else:

      if worker:
        worker.status = "spawn"

      jobs = []

      # Try to dequeue from each of the queues until we have filled max_jobs.
      for queue in queue_objects:

        queue_config = get_current_config().get("raw_queues", {}).get(queue.id, {})

        job_factory = queue_config.get("job_factory")
        if not job_factory and queue.is_raw:
          raise Exception("No job_factory configured for raw queue %s" % queue.id)

        params = []

        # ZSET with times
        if queue.is_timed:

          current_time = time.time()

          # When we have a pushback_seconds argument, we never pop items from the queue, instead
          # we push them back by an amount of time so that they don't get dequeued again until
          # the task finishes.
          if queue_config.get("pushback_seconds"):
            pushback_time = current_time + float(queue_config.get("pushback_seconds"))
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
          continue

        max_jobs -= len(params)

        if queue.is_raw:
          job_data = [job_factory(p) for p in params]
          for j in job_data:
            j["status"] = "started"
            j["queue"] = queue.id
            if worker:
              j["worker"] = worker.id

          jobs += job_class.insert(job_data)

        else:
          jobs += [job_class(_job_id, queue=queue, start=True)
                   for _job_id in params if _job_id]

        if max_jobs == 0:
          break

      for job in jobs:
        metric("queues.%s.dequeued" % job.queue, 1)
      metric("queues.all.dequeued", len(jobs))

      return jobs


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

    metric("jobs.status.queued", len(params_group))

    # TODO use Job.insert here too?
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
