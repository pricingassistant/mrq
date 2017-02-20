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

        return self.collection.count({"status": "queued", "queue": self.id})

    def list_job_ids(self, skip=0, limit=20):
        """ Returns a list of job ids on a queue """

        return [str(x) for x in self.collection.find(
            {"status": "queued"},
            sort=[("_id", -1 if self.is_reverse else 1)],
            projection={"_id": 1})
        ]

    def dequeue_jobs(self, max_jobs=1, job_class=None, worker=None):
        """ Fetch a maximum of max_jobs from this queue """

        if job_class is None:
            from .job import Job
            job_class = Job

        if worker:
            worker.status = "spawn"
            worker.idle_event.clear()

        count = 0

        for _ in range(max_jobs):

            job_data = self.collection.find_one_and_update(
                {
                    "status": "queued",
                    "queue": self.id
                },
                {"$set": {
                    "status": "started",
                    "datestarted": datetime.datetime.utcnow(),
                    "worker": worker.id if worker else None
                }},
                sort=[("_id", -1 if self.is_reverse else 1)],
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

            count += 1
            context.metric("queues.%s.dequeued" % job_data["queue"], 1)

            job = job_class(job_data["_id"], queue=self.id, start=False)
            job.set_data(job_data)
            job.datestarted = datetime.datetime.utcnow()

            context.metric("jobs.status.started")

            yield job

        context.metric("queues.all.dequeued", count)
