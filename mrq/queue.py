from .utils import load_task_class, group_iter
from .worker import get_current_worker
import time
from bson import ObjectId


def send_task(path, params, **kwargs):
  return send_tasks(path, [params], **kwargs)[0]


def send_tasks(path, params_list, queue=None, sync=False, batch_size=1000):

  if len(params_list) == 0:
    return []

  if sync:
    task_class = load_task_class(path)
    return [task_class().run(params) for params in params_list]

  if queue is None:
    queue = "default"
    # ROUTES[task]["queue"]

  worker = get_current_worker()
  if not worker:
    raise Exception("Can't queue task if worker is not initialized.")

  all_ids = []

  for params_group in group_iter(params_list, n=batch_size):

    collection = worker.mongodb_jobs.mrq_jobs
    redis = worker.redis

    job_ids = collection.insert([{
      "path": path,
      "params": params,
      "queue": queue
    } for params in params_group], w=1)

    # Between these 2 calls, a task can be inserted in MongoDB but not queued in Redis.
    # This is the same as dequeueing a task from Redis and being stopped before updating the "started"
    # flag in MongoDB.

    # TODO prefix
    redis.rpush(queue, *[str(x) for x in job_ids])

    all_ids += job_ids

  return all_ids


def wait_for_result(job_id, poll_interval=1, timeout=None):

  worker = get_current_worker()
  if not worker:
    raise Exception("Can't wait for task if worker is not initialized.")

  collection = worker.mongodb_jobs.mrq_jobs

  end_time = None
  if timeout:
    end_time = time.time() + timeout

  while (end_time is None or time.time() < end_time):

    job_data = collection.find_one({
      "_id": ObjectId(job_id),
      "status": "success"
    }, fields={
      "_id": 0,
      "result": 1
    })

    if job_data:
      return job_data.get("result")

    time.sleep(poll_interval)

  raise Exception("Waited for job result for %ss seconds, timeout." % timeout)

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
