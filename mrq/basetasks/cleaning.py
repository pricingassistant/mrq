from builtins import str
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


class RequeueRedisStartedJobs(Task):

    """ Requeue jobs that were started in Redis but not in Mongo.

        They could have been lost by a worker interrupt between
        redis.lpop and mongodb.update
    """

    max_concurrency = 1

    def run(self, params):

        redis_key_started = Queue.redis_key_started()

        stats = {
            "fetched": 0,
            "requeued": 0
        }

        # Fetch all the jobs started more than a minute ago - they should not
        # be in redis:started anymore
        job_ids = connections.redis.zrangebyscore(
            redis_key_started, "-inf", time.time() - params.get("timeout", 60))

        # TODO this should be wrapped inside Queue or Worker
        # we shouldn't access these internals here
        queue_obj = Queue("default")
        unserialized_job_ids = queue_obj.unserialize_job_ids(job_ids)

        for i, job_id in enumerate(job_ids):

            queue = Job(unserialized_job_ids[i], start=False, fetch=False).fetch(
                full_data=True).data["queue"]

            queue_obj = Queue(queue)

            stats["fetched"] += 1

            log.info("Requeueing %s on %s" % (unserialized_job_ids[i], queue))

            # TODO LUA script & don't rpush if not in zset anymore.
            with connections.redis.pipeline(transaction=True) as pipeline:
                pipeline.zrem(redis_key_started, job_id)
                pipeline.rpush(queue_obj.redis_key, job_id)
                pipeline.execute()

            stats["requeued"] += 1

        return stats


class RequeueLostJobs(Task):

    """ Requeue jobs that were queued but don't appear in Redis anymore.

        They could have been lost by a Redis flush or another severe issue
    """

    max_concurrency = 1

    def run(self, params):

        # If there are more than this much items on the queue, we don't try to check if our mongodb
        # jobs are still queued.
        max_queue_items = params.get("max_queue_items", 1000)

        stats = {
            "fetched": 0,
            "requeued": 0
        }

        # This was only checking in Redis and wasn't resistant to a redis-wide flush.
        # Doing Queue.all() is slower but covers more edge cases.
        # all_queues = Queue.all_known()

        all_queues = Queue.all()

        log.info("Checking %s queues" % len(all_queues))

        for queue_name in all_queues:

            queue = Queue(queue_name)
            queue_size = queue.size()

            # If the queue is raw, the jobs were only stored in redis so they are lost for good.
            if queue.is_raw:
                continue

            log.info("Checking queue %s" % queue_name)

            if queue_size > max_queue_items:
                log.info("Stopping because queue %s has %s items" %
                         (queue_name, queue_size))
                continue

            queue_jobs_ids = set(queue.list_job_ids(limit=max_queue_items + 1))
            if len(queue_jobs_ids) >= max_queue_items:
                log.info(
                    "Stopping because queue %s actually had more than %s items" %
                    (queue_name, len(queue_jobs_ids)))
                continue

            for job_data in connections.mongodb_jobs.mrq_jobs.find({
                "queue": queue_name,
                "status": "queued"
            }, projection={"_id": 1}).sort([["_id", 1]]):

                stats["fetched"] += 1

                if str(job_data["_id"]) in queue_jobs_ids:
                    log.info("Found job %s on queue %s. Stopping" % (job_data["_id"], queue.id))
                    break

                # At this point, this job is not on the queue and we're sure
                # the queue is less than max_queue_items
                # We can safely requeue the job.
                log.info("Requeueing %s on %s" % (job_data["_id"], queue.id))

                stats["requeued"] += 1
                job = Job(job_data["_id"])
                job.requeue(queue=queue_name)

        return stats


class MigrateKnownQueues(Task):
    """
        Migrate known_queues from old set format to new zset
    """

    max_concurrency = 1

    def run(self, params):
        key = "%s:known_queues" % get_current_config()["redis_prefix"]
        for queue in connections.redis.smembers(key):
            Queue(queue).add_to_known_queues()


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
        for queue, time_last_used in known_queues.iteritems():
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
