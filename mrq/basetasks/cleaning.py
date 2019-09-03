from future.builtins import str
from mrq.queue import Queue
from mrq.task import Task
from mrq.job import Job
from mrq.context import log, connections, run_task, get_current_config
import datetime
import time


class RequeueInterruptedJobs(Task):

    """ Requeue jobs that were marked as status=interrupt when a worker got a SIGTERM. """

    max_concurrency = 1

    def run(self, params):
        return run_task("mrq.basetasks.utils.JobAction", {
            "status": "interrupt",
            "action": "requeue_retry"
        })


class RequeueRetryJobs(Task):

    """ Requeue jobs that were marked as retry. """

    max_concurrency = 1

    def run(self, params):
        print("IN")
        return run_task("mrq.basetasks.utils.JobAction", {
            "status": "retry",
            "dateretry": {"$lte": datetime.datetime.utcnow()},
            "action": "requeue_retry"
        })


class QueueDelayedJobs(Task):

    """ Requeue jobs that were marked as delayed. """

    max_concurrency = 1

    def run(self, params):
        return run_task("mrq.basetasks.utils.JobAction", {
            "status": "delayed",
            "dateretry": {"$lte": datetime.datetime.utcnow()},
            "action": "requeue"
        })


class DeleteExpiresJobs(Task):

    """ Delete jobs that were dateexpires is less than the current date time. """

    max_concurrency = 1

    def run(self, params):
        return run_task("mrq.basetasks.utils.JobAction", {
            "status": ["success","cancel"],
            "dateexpires": {"$lte": datetime.datetime.utcnow()},
            "action": "delete"
        })


class RequeueStartedJobs(Task):

    """ Requeue jobs that were marked as status=started and never finished.

        That may be because the worker got a SIGKILL or was terminated abruptly.
        The timeout parameter of this task is in addition to the task's own timeout.
    """

    max_concurrency = 1

    def run(self, params):

        additional_timeout = params.get("timeout", 300)

        stats = {
            "requeued": 0,
            "started": 0
        }

        # There shouldn't be that much "started" jobs so we can quite safely
        # iterate over them.

        fields = {
            "_id": 1, "datestarted": 1, "queue": 1, "path": 1, "retry_count": 1, "worker": 1, "status": 1
        }
        for job_data in connections.mongodb_jobs.mrq_jobs.find(
                {"status": "started"}, projection=fields):
            job = Job(job_data["_id"])
            job.set_data(job_data)

            stats["started"] += 1

            expire_date = datetime.datetime.utcnow(
            ) - datetime.timedelta(seconds=job.timeout + additional_timeout)

            requeue = job_data["datestarted"] < expire_date

            if not requeue:
                # Check that the supposedly running worker still exists
                requeue = not connections.mongodb_jobs.mrq_workers.find_one(
                    {"_id": job_data["worker"]}, projection={"_id": 1})

            if requeue:
                log.debug("Requeueing job %s" % job.id)
                job.requeue()
                stats["requeued"] += 1

        return stats
