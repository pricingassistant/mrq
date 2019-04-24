from __future__ import print_function
from future.utils import itervalues
from future.builtins import str
from mrq.task import Task
from mrq.queue import Queue
from bson import ObjectId
from mrq.context import connections, get_current_config, get_current_job
from mrq.job import set_queues_size
from collections import defaultdict
from mrq.utils import group_iter
import datetime
import ujson as json


def get_task_cfg(taskpath):
    return get_current_config().get("tasks", {}).get(taskpath) or {}


class JobAction(Task):

    params = None
    collection = None

    def run(self, params):

        self.params = params
        self.collection = connections.mongodb_jobs.mrq_jobs

        query = self.build_query()

        return self.perform_action(
            self.params.get("action"), query, self.params.get("destination_queue")
        )

    def build_query(self):
        query = {}
        current_job = get_current_job()

        if self.params.get("id"):
            query["_id"] = ObjectId(self.params.get("id"))

        # TODO use redis for queue
        for k in [
                "queue",
                "status",
                "worker",
                "path",
                "dateexpires",
                "dateretry",
                "exceptiontype"]:
            if self.params.get(k):
                if isinstance(self.params[k], (list, tuple)):
                    query[k] = {"$in": list(self.params[k])}
                else:
                    query[k] = self.params[k]
            if query.get("worker"):
                query["worker"] = ObjectId(query["worker"])

        if self.params.get("params"):
            params_dict = json.loads(self.params.get("params"))  # pylint: disable=no-member

            for key in params_dict:
                query["params.%s" % key] = params_dict[key]

        if current_job and "_id" not in query:
            query["_id"] = {"$lte": current_job.id}

        return query

    def perform_action(self, action, query, destination_queue):

        stats = {
            "requeued": 0,
            "cancelled": 0,
            "deleted": 0
        }

        if action == "cancel":

            default_job_timeout = get_current_config()["default_job_timeout"]

            # Finding the ttl here to expire is a bit hard because we may have mixed paths
            # and hence mixed ttls.
            # If we are cancelling by path, get this ttl
            if query.get("path"):
                result_ttl = get_task_cfg(query["path"]).get("result_ttl", default_job_timeout)

            # If not, get the maxmimum ttl of all tasks.
            else:

                tasks_defs = get_current_config().get("tasks", {})
                tasks_ttls = [cfg.get("result_ttl", 0) for cfg in itervalues(tasks_defs)]

                result_ttl = max([default_job_timeout] + tasks_ttls)

            now = datetime.datetime.utcnow()

            size_by_queues = defaultdict(int)
            if "queue" not in query:
                for job in self.collection.find(query, projection={"queue": 1}):
                    size_by_queues[job["queue"]] += 1

            ret = self.collection.update(query, {"$set": {
                "status": "cancel",
                "dateexpires": now + datetime.timedelta(seconds=result_ttl),
                "dateupdated": now
            }}, multi=True)
            stats["cancelled"] = ret["n"]

            if "queue" in query:
                if isinstance(query["queue"], str):
                    size_by_queues[query["queue"]] = ret["n"]
            set_queues_size(size_by_queues, action="decr")

            # Special case when emptying just by queue name: empty it directly!
            # In this case we could also loose some jobs that were queued after
            # the MongoDB update. They will be "lost" and requeued later like the other case
            # after the Redis BLPOP
            if list(query.keys()) == ["queue"]:
                Queue(query["queue"]).empty()

        elif action in ("requeue", "requeue_retry"):

            # Requeue task by groups of maximum 1k items (if all in the same
            # queue)
            cursor = self.collection.find(query, projection=["_id", "queue"])

            # We must freeze the list because queries below would change it.
            # This could not fit in memory, research adding {"stats": {"$ne":
            # "queued"}} in the query
            fetched_jobs = list(cursor)

            for jobs in group_iter(fetched_jobs, n=1000):

                jobs_by_queue = defaultdict(list)
                for job in jobs:
                    jobs_by_queue[job["queue"]].append(job["_id"])
                    stats["requeued"] += 1

                for queue in jobs_by_queue:
                    updates = {
                        "status": "queued",
                        "datequeued": datetime.datetime.utcnow(),
                        "dateupdated": datetime.datetime.utcnow()
                    }

                    if destination_queue is not None:
                        updates["queue"] = destination_queue

                    if action == "requeue":
                        updates["retry_count"] = 0

                    self.collection.update({
                        "_id": {"$in": jobs_by_queue[queue]}
                    }, {"$set": updates}, multi=True)

                set_queues_size({queue: len(jobs) for queue, jobs in jobs_by_queue.iteritems()})
        elif action == 'delete':
            amount_delete = self.collection.delete_many(query)
            stats["deleted"] = amount_delete.deleted_count
        return stats

