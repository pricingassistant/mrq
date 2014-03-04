from mrq.task import Task
from mrq.job import Job
from bson import ObjectId
from mrq.context import log, connections


class JobAction(Task):

  def run(self, params):

    self.params = params
    self.collection = connections.mongodb_jobs.mrq_jobs

    query = self.build_query()
    jobs = self.fetch_jobs(query)
    return self.perform_action(self.params.get("action"), jobs)

  def build_query(self):
    query = {}
    if self.params.get("id"):
      query["_id"] = ObjectId(self.params.get("id"))

    for k in ["queue", "status", "worker", "path", "dateretry"]:  # TODO use redis for queue
      if self.params.get(k):
        query[k] = self.params.get(k)

    return query

  def fetch_jobs(self, query):
    return self.collection.find(query, fields=["_id"])

  def perform_action(self, action, jobs):

    stats = {
      "requeued": 0,
      "cancelled": 0,
      "fetched": 0
    }

    for job_data in jobs:
      stats["fetched"] += 1
      job = Job(job_data["_id"], fetch=True)

      if action == "requeue":
        stats["requeued"] += 1
        job.requeue()
      elif action == "cancel":
        stats["cancelled"] += 1
        job.cancel()

    return stats
