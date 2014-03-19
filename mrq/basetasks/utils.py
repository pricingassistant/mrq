from mrq.task import Task
from mrq.job import Job
from mrq.queue import Queue
from bson import ObjectId
from mrq.context import connections, get_current_config
from collections import defaultdict
from mrq.utils import group_iter
import datetime
import ujson as json


class JobAction(Task):

  def run(self, params):

    self.params = params
    self.collection = connections.mongodb_jobs.mrq_jobs

    query = self.build_query()

    return self.perform_action(self.params.get("action"), query)

  def build_query(self):
    query = {}
    if self.params.get("id"):
      query["_id"] = ObjectId(self.params.get("id"))

    for k in ["queue", "status", "worker", "path", "dateretry", "exceptiontype"]:  # TODO use redis for queue
      if self.params.get(k):
        query[k] = self.params.get(k)

    if self.params.get("params"):
      params_dict = json.loads(self.params.get("params"))

      for key in params_dict.keys():
        query["params.%s" % key] = params_dict[key]

    return query

  def perform_action(self, action, query):

    stats = {
      "requeued": 0,
      "cancelled": 0
    }

    if action == "cancel":

      # Finding the ttl here to expire is a bit hard because we may have mixed paths
      # and hence mixed ttls.
      # If we are cancelling by path, get this ttl
      if query.get("path"):

        task_def = get_current_config().get("tasks", {}).get(query["path"]) or {}
        result_ttl = task_def.get("result_ttl", Job.result_ttl)

      # If not, get the maxmimum ttl of all tasks.
      else:
        result_ttl = max([Job.result_ttl] + [task_def.get("result_ttl", Job.result_ttl) for task_def in get_current_config().get("tasks", {}).values()])

      now = datetime.datetime.utcnow()
      ret = self.collection.update(query, {"$set": {
        "status": "cancel",
        "dateexpires": now + datetime.timedelta(seconds=result_ttl),
        "dateupdated": now
      }}, multi=True)
      stats["cancelled"] = ret["n"]

      # Special case when emptying just by queue name: empty it directly!
      # In this case we could also loose some jobs that were queued after
      # the MongoDB update. They will be "lost" and requeued later like the other case
      # after the Redis BLPOP
      if query.keys() == ["queue"]:
        Queue(query["queue"]).empty()

    elif action == "requeue":

      # Requeue task by groups of maximum 1k items (if all in the same queue)
      cursor = self.collection.find(query, fields=["_id", "queue"])

      # We must freeze the list because queries below would change it.
      # This could not fit in memory, research adding {"stats": {"$ne": "queued"}} in the query
      fetched_jobs = list(cursor)

      for jobs in group_iter(fetched_jobs, n=1000):

        jobs_by_queue = defaultdict(list)
        for job in jobs:
          jobs_by_queue[job["queue"]].append(job["_id"])
          stats["requeued"] += 1

        for queue in jobs_by_queue:
          self.collection.update({
            "_id": {"$in": jobs_by_queue[queue]}
          }, {"$set": {
            "status": "queued",
            "dateupdated": datetime.datetime.utcnow()
          }}, multi=True)

          # Between these two lines, jobs can become "lost" too.

          Queue(queue).enqueue_job_ids([str(x) for x in jobs_by_queue[queue]])

    return stats
