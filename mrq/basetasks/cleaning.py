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
        return run_task("mrq.basetasks.utils.JobAction", {
            "status": "retry",
            "dateretry": {"$lte": datetime.datetime.utcnow()},
            "action": "requeue_retry"
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

        fields = {"_id": 1, "datestarted": 1, "queue": 1, "path": 1, "retry_count": 1}
        for job_data in connections.mongodb_jobs.mrq_jobs.find(
                {"status": "started"}, projection=fields):
            job = Job(job_data["_id"])
            job.set_data(job_data)

            stats["started"] += 1

            expire_date = datetime.datetime.utcnow(
            ) - datetime.timedelta(seconds=job.timeout + additional_timeout)

            if job_data["datestarted"] < expire_date:
                log.debug("Requeueing job %s" % job.id)
                job.requeue()
                stats["requeued"] += 1

        return stats


class MigrateKnownQueues(Task):
    """
        Migrate known_queues from old set format to new zset
    """

    max_concurrency = 1

    def run(self, params):
        key = "%s:known_queues" % get_current_config()["redis_prefix"]
        for queue in connections.redis.smembers(key):
            Queue(str(queue)).add_to_known_queues()


class CleanKnownQueues(Task):

    """
        Cleans the known queues in Redis.

        To be deleted, a queue must:
         - not have been used in the last 7 days
         - be empty
    """

    max_concurrency = 1

    def run(self, params):

        max_age = int(params.get("max_age") or (7 * 86400))
        pretend = bool(params.get("pretend"))
        check_mongo = bool(params.get("check_mongo"))

        known_queues = Queue.redis_known_queues()

        removed_queues = []

        queues_from_config = Queue.all_known_from_config()

        print("Found %s known queues & %s from config" % (len(known_queues), len(queues_from_config)))

        # Only clean queues older than N days
        time_threshold = time.time() - max_age
        for queue, time_last_used in known_queues.items():
            if queue in queues_from_config:
                continue
            if time_last_used < time_threshold:
                q = Queue(queue, add_to_known_queues=False)
                size = q.size()
                if check_mongo:
                    size += connections.mongodb_jobs.mrq_jobs.count({"queue": queue})
                if size == 0:
                    removed_queues.append(queue)
                    print("Removing empty queue '%s' from known queues ..." % queue)
                    if not pretend:
                        q.remove_from_known_queues()

        print("Cleaned %s queues" % len(removed_queues))

        return removed_queues
