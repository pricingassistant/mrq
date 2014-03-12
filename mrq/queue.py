from .utils import load_class_by_path, group_iter
from .context import connections, get_current_config


class Queue(object):

  def __init__(self, queue_id):
    if isinstance(queue_id, Queue):
      self.id = queue_id.id  # TODO use __new__?
    else:
      self.id = queue_id

  @property
  def redis_key(self):
    return "%s:q:%s" % (get_current_config()["redis_prefix"], self.id)

  def enqueue_job_ids(self, job_ids):
    connections.redis.rpush(self.redis_key, *job_ids)

  def size(self):
    return connections.redis.llen(self.redis_key)

  def empty(self):
    return connections.redis.delete(self.redis_key)

  def list_job_ids(self, skip=0, limit=20):
    return connections.redis.lrange(self.redis_key, skip, skip + limit - 1)

  @classmethod
  def all(cls):
    # TODO MongoDB distinct?
    prefix = get_current_config()["redis_prefix"]
    queues = []
    for key in connections.redis.keys():
      if key.startswith(prefix):
        queues.append(Queue(key[len(prefix) + 3:]))

    return queues


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

  for params_group in group_iter(params_list, n=batch_size):

    collection = connections.mongodb_jobs.mrq_jobs

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


    # # Cf code in send_task
    # if kwargs.get("uniquestarted") or kwargs.get("uniquequeued"):
    #   descriptions = {json.dumps([task, params, {}]):i for i, params in enumerate(params_list)}
    #   cancelled_indexes = []

    #   if kwargs.get("uniquestarted"):
    #     del kwargs["uniquestarted"]
    #     started_queue = Queue("started", connection=connection)

    #     for job in started_queue.jobs:
    #       if job.description in descriptions:
    #         cancelled_indexes.append(descriptions.get(job.description))

    #   if kwargs.get("uniquequeued"):
    #     params_list = uniqify_params_list(params_list)
    #     del kwargs["uniquequeued"]
    #     started_queue = Queue(task, connection=connection)

    #     for job in started_queue.jobs:
    #       if job.description in descriptions and job.status != "finished":
    #         cancelled_indexes.append(descriptions.get(job.description))

    #   # http://stackoverflow.com/questions/11303225/how-to-remove-multiple-indexes-from-a-list-at-the-same-time
    #   for i in sorted(set(cancelled_indexes), reverse=True):
    #     params_list.pop(i)
