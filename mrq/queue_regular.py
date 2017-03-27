from .queue import Queue
from . import context
import datetime
from pymongo.collection import ReturnDocument


class QueueRegular(Queue):

    @property
    def collection(self):
        return context.connections.mongodb_jobs.mrq_jobs

    def size(self):
        """ Returns the total number of queued jobs on the queue """

        if self.id.endswith("/"):
            subqueues = list(self.get_known_subqueues())
            if len(subqueues) == 0:
                return 0
            else:
                return self.collection.count({"status": "queued", "queue": {"$in": subqueues}})
        else:
            return self.collection.count({"status": "queued", "queue": self.id})

    def list_job_ids(self, skip=0, limit=20):
        """ Returns a list of job ids on a queue """

        return [str(x["_id"]) for x in self.collection.find(
            {"status": "queued"},
            sort=[("_id", -1 if self.is_reverse else 1)],
            projection={"_id": 1})
        ]

    def dequeue_jobs(self, max_jobs=1, job_class=None, worker=None):
        """ Fetch a maximum of max_jobs from this queue """

        if job_class is None:
            from .job import Job
            job_class = Job

        count = 0

        job_ids = None

        # TODO: remove _id sort after full migration to datequeued
        sort_order = [("datequeued", -1 if self.is_reverse else 1), ("_id", -1 if self.is_reverse else 1)]

        # MongoDB optimization: with many jobs it's faster to fetch the IDs first and do the atomic update second
        # Some jobs may have been stolen by another worker in the meantime but it's a balance (should we over-fetch?)
        if max_jobs > 5:
            job_ids = [x["_id"] for x in self.collection.find(
                {
                    "status": "queued",
                    "queue": self.id
                },
                limit=max_jobs,
                sort=sort_order,
                projection={"_id": 1}
            )]

            if len(job_ids) == 0:
                return

        for i in range(max_jobs if job_ids is None else len(job_ids)):

            query = {
                "status": "queued",
                "queue": self.id
            }
            if job_ids is not None:
                query = {
                    "status": "queued",
                    "_id": job_ids[i]
                }

            job_data = self.collection.find_one_and_update(
                query,
                {"$set": {
                    "status": "started",
                    "datestarted": datetime.datetime.utcnow(),
                    "worker": worker.id if worker else None
                }},
                sort=sort_order,
                return_document=ReturnDocument.AFTER,
                projection={
                    "_id": 1,
                    "path": 1,
                    "params": 1,
                    "status": 1,
                    "retry_count": 1,
                    "queue": 1
                }
            )

            if not job_data:
                break

            if worker:
                worker.status = "spawn"

            count += 1
            context.metric("queues.%s.dequeued" % job_data["queue"], 1)

            job = job_class(job_data["_id"], queue=self.id, start=False)
            job.set_data(job_data)
            job.datestarted = datetime.datetime.utcnow()

            context.metric("jobs.status.started")

            yield job

        context.metric("queues.all.dequeued", count)
