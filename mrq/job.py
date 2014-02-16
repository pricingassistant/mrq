import datetime
from bson import ObjectId
import pymongo
from .exceptions import RetryInterrupt
from .utils import load_task_class


class Job(object):

  timeout = 300

  # Exceptions that don't mark the task as failed but as retry
  retry_on_exceptions = [
    pymongo.errors.AutoReconnect,
    pymongo.errors.OperationFailure,
    pymongo.errors.ConnectionFailure,
    RetryInterrupt
  ]

  def __init__(self, job_id, worker=None, queue=None, start=False):
    self.worker = worker
    self.queue = queue

    self.log = self.worker.log_handler.get_logger(job=self)

    self.collection = self.worker.mongodb_jobs.mrq_jobs
    self.id = job_id

    self.task = None

    if start:
      self.fetch_and_start()

  def fetch_and_start(self):
    """ Get the current job data and flag it as started. """

    self.data = self.collection.find_and_modify({
      "_id": ObjectId(self.id)
    }, {"$set": {
      "status": "started",
      "datestarted": datetime.datetime.utcnow(),
      "worker": self.worker.name
    }}, fields={
      "_id": 0,
      "path": 1,
      "params": 1
    })

    if self.data is None:
      raise Exception("Job not found in MongoDB!")

  def _save(self, changes):

    self.collection.update({
      "_id": ObjectId(self.id)
    }, {"$set": changes}, w=1)

  def save_result(self, result=None):

    self._save({
      "result": result,
      "datefinished": datetime.datetime.utcnow(),
      "status": "success"
    })

  def save_error(self, traceback=None):

    self._save({
      "traceback": traceback,
      "status": "failed",
      "datefinished": datetime.datetime.utcnow()
    })

  def save_timeout(self):

    self._save({
      "status": "timeout",
      "datefinished": datetime.datetime.utcnow()
    })

  def save_retry(self, eta=None, traceback=None):

    if not eta:
      eta = datetime.timedelta(hours=24)

    self._save({
      "traceback": traceback,
      "status": "retry",
      "dateretry": datetime.datetime.utcnow() + eta
    })

  def get_retry_eta_for_exception(self, exc):
    """ Decides on how long to wait before requeueing a job, depending on which exception
        interrupted it.
    """

    if isinstance(exc, RetryInterrupt):
      return exc.eta

    return 24 * 3600

  def perform(self):
    """ Loads and starts the main task for this job, the saves the result. """

    task_class = load_task_class(self.data["path"])

    self.task = task_class(job=self)

    result = self.task.run(self.data["params"])

    self.save_result(result)
