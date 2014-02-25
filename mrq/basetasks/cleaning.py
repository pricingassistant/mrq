from mrq.queue import send_task
from mrq.task import Task
from mrq.job import Job
from mrq.context import log
import datetime


class RequeueInterruptedJobs(Task):
  """ Requeue jobs that were marked as status=interrupt when a worker got a SIGTERM. """

  def run(self, params):
    send_task("mrq.basetasks.utils.JobAction", {
      "status": "interrupt",
      "action": "requeue"
    })


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
    self.collection = self.job.worker.mongodb_jobs.mrq_jobs
    for job_data in self.collection.find({"status": "started"}, fields={"_id": 1, "datestarted": 1}):
      job = Job(job_data["_id"])

      stats["started"] += 1

      expire_date = datetime.datetime.utcnow() - datetime.timedelta(seconds=job.timeout + additional_timeout)
      if job_data["datestarted"] < expire_date:
        log.debug("Requeueing job %s" % job.id)
        job.requeue()
        stats["requeued"] += 1

    return stats


class RequeueLostJobs(Task):
  """ Requeue jobs that were queued but don't appear in Redis anymore.

      They could have been lost either by a Redis flush or by a worker interrupt between
      redis.blpop and mongodb.update
  """

  def run(self, params):

    self.collection = self.job.worker.mongodb_jobs.mrq_jobs

    for job_data in self.collection.find({"status": "queued"}).sort([{"_id": 1}]).batch_size(100):


