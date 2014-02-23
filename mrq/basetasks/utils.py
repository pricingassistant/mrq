from mrq.task import Task
from mrq.job import Job
from bson import ObjectId
from mrq.context import log


class JobAction(Task):

  def run(self, params):

    self.params = params
    self.collection = self.job.worker.mongodb_jobs.mrq_jobs

    query = self.build_query()
    jobs = self.fetch_jobs(query)
    self.perform_action(self.params.get("action"), jobs)

  def build_query(self):
    query = {}
    if self.params.get("id"):
      query["_id"] = ObjectId(self.params.get("id"))

    for k in ["queue", "status", "worker", "path"]:  # TODO use redis for queue
      if self.params.get(k):
        query[k] = self.params.get(k)

    return query

  def fetch_jobs(self, query):
    return self.collection.find(query, fields=["_id"])

  def perform_action(self, action, jobs):

    for job_data in jobs:
      job = Job(job_data["_id"])

      if action == "requeue":
        job.requeue()
      elif action == "cancel":
        job.cancel()
