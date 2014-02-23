import datetime
from bson import ObjectId
import pymongo
from .exceptions import RetryInterrupt
from .utils import load_task_class
from .queue import send_task
from .context import get_current_worker, log


class Job(object):

  # Seconds the job can last before timeouting
  timeout = 300

  # Seconds the results are kept in MongoDB
  result_ttl = 7 * 24 * 3600

  # Exceptions that don't mark the task as failed but as retry
  retry_on_exceptions = (
    pymongo.errors.AutoReconnect,
    pymongo.errors.OperationFailure,
    pymongo.errors.ConnectionFailure,
    RetryInterrupt
  )

  def __init__(self, job_id, worker=None, queue=None, start=False, fetch=False):
    self.worker = get_current_worker()
    self.queue = queue
    self.datestarted = datetime.datetime.utcnow()

    self.collection = self.worker.mongodb_jobs.mrq_jobs
    self.id = ObjectId(job_id)

    self.data = None
    self.task = None

    if start:
      self.fetch(start=True)
    elif fetch:
      self.fetch(start=False)

  def fetch(self, start=False):
    """ Get the current job data and possibly flag it as started. """

    fields = {
      "_id": 0,
      "path": 1,
      "params": 1,
      "status": 1,
      "_id": 1
    }

    if start:
      self.data = self.collection.find_and_modify({
        "_id": self.id,
        "status": {"$nin": ["cancel"]}
      }, {"$set": {
        "status": "started",
        "datestarted": datetime.datetime.utcnow(),
        "worker": self.worker.name
      }}, fields=fields)
    else:
      self.data = self.collection.find_one({
        "_id": self.id
      }, fields=fields)

    if self.data is None:
      log.info("Job %s not found in MongoDB or status was cancelled!" % self.id)
    else:
      task_def = self.worker.config.get("tasks", {}).get(self.data["path"]) or {}
      self.timeout = task_def.get("timeout", self.timeout)
      self.result_ttl = task_def.get("result_ttl", self.result_ttl)

  def save_status(self, status, result=None, traceback=None, w=1):

    updates = {
      "status": status,
      "dateupdated": datetime.datetime.utcnow()
    }
    if result is not None:
      updates["result"] = result
    if traceback is not None:
      updates["traceback"] = traceback

    # Make the job document expire
    if status in ("success", "cancel"):
      updates["dateexpires"] = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.result_ttl)

    self.collection.update({
      "_id": self.id
    }, {"$set": updates}, w=w)

  def save_retry(self, exc, traceback=None):

    countdown = 24 * 3600

    if isinstance(exc, RetryInterrupt) and exc.countdown:
      countdown = exc.countdown

    update = {
      "traceback": traceback,
      "status": "retry",
      "dateupdated": datetime.datetime.utcnow(),
      "dateretry": datetime.datetime.utcnow() + datetime.timedelta(seconds=countdown)
    }

    if isinstance(exc, RetryInterrupt) and exc.queue:
      update["queue"] = exc.queue

    self.collection.update({
      "_id": self.id
    }, {"$set": update}, w=1)

  def retry(self, queue=None, countdown=None, max_retries=None):

    exc = RetryInterrupt()
    exc.queue = queue
    exc.countdown = countdown
    raise exc

  def cancel(self):
    self.save_status("cancel")

  def requeue(self, queue=None):
    self.save_status(None)

    # TODO factor elsewhere
    self.worker.redis.rpush(queue or self.data["queue"], str(self.id))

  def perform(self):
    """ Loads and starts the main task for this job, the saves the result. """

    if self.data is None:
      return

    task_class = load_task_class(self.data["path"])

    self.task = task_class(job=self)

    result = self.task.run(self.data["params"])

    self.save_status("success", result)
