from mrq.queue import send_task, Queue
from mrq.task import Task
from mrq.job import Job
from mrq.context import log, connections
import datetime
import time


class RequeueInterruptedJobs(Task):
  """ Requeue jobs that were marked as status=interrupt when a worker got a SIGTERM. """

  def run(self, params):
    return send_task("mrq.basetasks.utils.JobAction", {
      "status": "interrupt",
      "action": "requeue"
    }, sync=True)


class RequeueRetryJobs(Task):
  """ Requeue jobs that were marked as retry. """

  def run(self, params):
    return send_task("mrq.basetasks.utils.JobAction", {
      "status": "retry",
      "dateretry": {"$lte": datetime.datetime.utcnow()},
      "action": "requeue"
    }, sync=True)


class RequeueStartedJobs(Task):
  """ Requeue jobs that were marked as status=started and never finished.

      That may be because the worker got a SIGKILL or was terminated abruptly. The timeout parameter
      of this task is in addition to the task's own timeout.
  """

  def run(self, params):

    additional_timeout = params.get("timeout", 300)

    stats = {
      "requeued": 0,
      "started": 0
    }

    # There shouldn't be that much "started" jobs so we can quite safely iterate over them.
    self.collection = connections.mongodb_jobs.mrq_jobs
    for job_data in self.collection.find({"status": "started"}, fields={"_id": 1, "datestarted": 1, "queue": 1, "path": 1}):
      job = Job(job_data["_id"])
      job.set_data(job_data)

      stats["started"] += 1

      expire_date = datetime.datetime.utcnow() - datetime.timedelta(seconds=job.timeout + additional_timeout)

      if job_data["datestarted"] < expire_date:
        log.debug("Requeueing job %s" % job.id)
        job.requeue()
        stats["requeued"] += 1

    return stats


class RequeueRedisStartedJobs(Task):
  """ Requeue jobs that were started in Redis but not in Mongo.

      They could have been lost by a worker interrupt between
      redis.lpop and mongodb.update
  """

  def run(self, params):

    self.collection = connections.mongodb_jobs.mrq_jobs

    redis_key_started = Queue.redis_key_started()

    stats = {
      "fetched": 0,
      "requeued": 0
    }

    # Fetch all the jobs started more than a minute ago - they should not be in redis:started anymore
    job_ids = connections.redis.zrangebyscore(redis_key_started, "-inf", time.time() - params.get("timeout", 60))

    for job_id in job_ids:

      queue = Job(job_id, start=False, fetch=False).fetch(full_data=True).data["queue"]

      stats["fetched"] += 1

      log.info("Requeueing %s on %s" % (job_id, queue))

      # TODO LUA script & don't rpush if not in zset anymore.
      with connections.redis.pipeline(transaction=True) as pipeline:
        pipeline.zrem(redis_key_started, job_id)
        pipeline.rpush(Queue(queue).redis_key, job_id)
        pipeline.execute()

      stats["requeued"] += 1

    return stats


class RequeueLostJobs(Task):
  """ Requeue jobs that were queued but don't appear in Redis anymore.

      They could have been lost either by a Redis flush
  """

  def run(self, params):

    self.collection = connections.mongodb_jobs.mrq_jobs

    # If there are more than this much items on the queue, we don't try to check if our mongodb
    # jobs are still queued.
    max_queue_items = params.get("max_queue_items", 1000)

    stats = {
      "fetched": 0,
      "requeued": 0
    }

    for job_data in self.collection.find({
      "status": "queued"
    }, fields={"_id": 1, "queue": 1}).sort([("_id", 1)]):

      stats["fetched"] += 1

      queue = Queue(job_data["queue"])
      queue_size = queue.size()
      if queue_size > max_queue_items:
        log.info("Stopping because queue %s has %s items" % (queue, queue_size))
        break

      queue_jobs_ids = set(queue.list_job_ids(limit=max_queue_items + 1))
      if len(queue_jobs_ids) >= max_queue_items:
        log.info("Stopping because queue %s actually had more than %s items" % (queue, len(queue_jobs_ids)))
        break

      if str(job_data["_id"]) in queue_jobs_ids:
        log.info("Stopping because we found job %s in redis" % job_data["_id"])
        break

      # At this point, this job is not on the queue and we're sure the queue is less than max_queue_items
      # We can safely requeue the job.
      log.info("Requeueing %s on %s" % (job_data["_id"], queue.id))

      stats["requeued"] += 1
      job = Job(job_data["_id"])
      job.requeue(queue=job_data["queue"])

    return stats


# class CleanupStoppedWorkers(Task):
#   """ Remove stopped or timeouted workers from mongodb.mrq_workers. """

#   def run(self, params):

