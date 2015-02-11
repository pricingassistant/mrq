import datetime
from bson import ObjectId
import time
from .exceptions import RetryInterrupt, MaxRetriesInterrupt, AbortInterrupt
from .utils import load_class_by_path, group_iter
import gevent
import objgraph
import random
import gc
from collections import defaultdict
import traceback
import sys

from . import context


class Job(object):

    timeout = None
    result_ttl = None
    max_retries = None
    retry_delay = None

    # All values above can be overrided from the TASKS config

    progress = 0

    _memory_stop = 0
    _memory_start = 0

    _current_io = None

    def __init__(self, job_id, queue=None, start=False, fetch=False):
        self.worker = context.get_current_worker()
        self.queue = queue
        self.datestarted = None

        self.collection = context.connections.mongodb_jobs.mrq_jobs

        if job_id is None:
            self.id = None
        else:
            self.id = ObjectId(job_id)

        self.data = None
        self.saved = True

        self.task = None
        self.greenlet_switches = 0
        self.greenlet_time = 0

        self._trace_mongodb = defaultdict(int)

        if start:
            self.fetch(start=True, full_data=False)
        elif fetch:
            self.fetch(start=False, full_data=False)

    def exists(self):
        """ Returns True if a job with the current _id exists in MongoDB. """
        return bool(self.collection.find_one({"_id": self.id}, fields={"_id": 1}))

    def fetch(self, start=False, full_data=True):
        """ Get the current job data and possibly flag it as started. """

        if self.id is None:
            return self

        if full_data is True:
            fields = None
        elif isinstance(full_data, dict):
            fields = full_data
        else:
            fields = {
                "_id": 0,
                "path": 1,
                "params": 1,
                "status": 1,
                "retry_count": 1
            }

        if start:

            self.datestarted = datetime.datetime.utcnow()
            self.set_data(self.collection.find_and_modify(
                {
                    "_id": self.id,
                    "status": {"$nin": ["cancel", "abort", "maxretries"]}
                },
                {"$set": {
                    "status": "started",
                    "datestarted": self.datestarted,
                    "worker": self.worker.id
                }},
                fields=fields)
            )

            context.metric("jobs.status.started")

        else:
            self.set_data(self.collection.find_one({
                "_id": self.id
            }, fields=fields))

        if self.data is None:
            context.log.info(
                "Job %s not found in MongoDB or status was cancelled!" %
                self.id)

        return self

    def set_data(self, data):
        self.data = data
        if self.data is None:
            return

        if "path" in self.data:
            cfg = context.get_current_config()
            task_def = cfg.get("tasks", {}).get(
                self.data["path"]
            ) or {}

            self.timeout = task_def.get("timeout", cfg["default_job_timeout"])
            self.result_ttl = task_def.get("result_ttl", cfg["default_job_result_ttl"])
            self.max_retries = task_def.get("max_retries", cfg["default_job_max_retries"])
            self.retry_delay = task_def.get("retry_delay", cfg["default_job_retry_delay"])

    def set_progress(self, ratio, save=False):
        self.data["progress"] = ratio
        self.saved = False

        # If not saved, will be updated in the next worker report
        if save:
            self.save()

    def save(self):
        """ Persists the current job metadata to MongoDB. Will be called at each worker report. """

        if not self.saved and self.data and "progress" in self.data:
            # TODO should we save more fields?
            self.collection.update({"_id": self.id}, {"$set": {
                "progress": self.data["progress"]
            }})
            self.saved = True

    @classmethod
    def insert(cls, jobs_data, queue=None, return_jobs=True, w=1):
        """ Insert a job into MongoDB """

        now = datetime.datetime.utcnow()
        for data in jobs_data:
            if data["status"] == "started":
                data["datestarted"] = now

        inserted = context.connections.mongodb_jobs.mrq_jobs.insert(
            jobs_data,
            manipulate=True,
            w=w
        )

        if return_jobs:
            jobs = []
            for data in jobs_data:
                job = cls(data["_id"], queue=queue)
                job.set_data(data)
                if data["status"] == "started":
                    job.datestarted = data["datestarted"]
                jobs.append(job)

            return jobs
        else:
            return inserted

    def _attach_original_exception(self, exc):
        """ Often, a retry will be raised inside an "except" block.
            This Keep track of the first exception for debugging purposes. """

        original_exception = sys.exc_info()
        if original_exception[0] is not None:
            exc.original_exception = original_exception

    def retry(self, queue=None, delay=None, max_retries=None):
        """ Marks the current job as needing to be retried. Interrupts it. """

        max_retries = max_retries
        if max_retries is None:
            max_retries = self.max_retries

        if self.data.get("retry_count", 0) >= max_retries:
            raise MaxRetriesInterrupt()

        exc = RetryInterrupt()

        exc.queue = queue or self.data.get("queue") or "default"
        exc.retry_count = self.data.get("retry_count", 0) + 1
        exc.delay = delay
        if exc.delay is None:
            exc.delay = self.retry_delay

        self._attach_original_exception(exc)

        raise exc

    def abort(self):
        """ Aborts the current task mid-excution. """
        exc = AbortInterrupt()
        self._attach_original_exception(exc)
        raise exc

    def cancel(self):
        """ Markes the current job as cancelled. Doesn't interrupt it. """
        self._save_status("cancel")

    def requeue(self, queue=None, retry_count=0):
        """ Requeues the current job. Doesn't interrupt it """

        if not queue:
            if not self.data or not self.data.get("queue"):
                self.fetch(full_data={"_id": 0, "queue": 1, "path": 1})
            queue = self.data["queue"]

        from .queue import Queue
        queue_obj = Queue(queue)

        self._save_status("queued", updates={
            "queue": queue,
            "retry_count": retry_count
        })

        # Between these two lines, jobs can become "lost" too.

        queue_obj.enqueue_job_ids([str(self.id)])

    def perform(self):
        """ Loads and starts the main task for this job, the saves the result. """

        if self.data is None:
            return

        context.log.debug("Starting %s(%s)" % (self.data["path"], self.data["params"]))
        task_class = load_class_by_path(self.data["path"])

        self.task = task_class()

        self.task.is_main_task = True

        result = self.task.run_wrapped(self.data["params"])

        self.save_success(result)

        if context.get_current_config().get("trace_greenlets"):

            # TODO: this is not the exact greenlet_time measurement because it doesn't
            # take into account the last switch's time. This is why we force a last switch.
            # This does cause a performance overhead. Instead, we should print the
            # last timing directly from the trace() function in context?

            # pylint: disable=protected-access

            gevent.sleep(0)
            current_greenlet = gevent.getcurrent()
            t = (datetime.datetime.utcnow() - self.datestarted).total_seconds()

            context.log.debug(
                "Job %s success: %0.6fs total, %0.6fs in greenlet, %s switches" %
                (self.id,
                 t,
                 current_greenlet._trace_time,
                 current_greenlet._trace_switches - 1)
            )

        else:
            context.log.debug("Job %s success: %0.6fs total" % (
                self.id, (datetime.datetime.utcnow() -
                          self.datestarted).total_seconds()
            ))

        return result

    def wait(self, poll_interval=1, timeout=None, full_data=False):
        """ Wait for this job to finish. """

        end_time = None
        if timeout:
            end_time = time.time() + timeout

        while end_time is None or time.time() < end_time:

            job_data = self.collection.find_one({
                "_id": ObjectId(self.id),
                "status": {"$nin": ["started", "queued"]}
            }, fields=({
                "_id": 0,
                "result": 1,
                "status": 1
            } if not full_data else None))

            if job_data:
                return job_data

            time.sleep(poll_interval)

        raise Exception(
            "Waited for job result for %ss seconds, timeout." % timeout)

    def save_retry(self, retry_exc):

        # If delay=0, requeue right away, don't go through the "retry" status
        if retry_exc.delay == 0:
            self.requeue(queue=retry_exc.queue, retry_count=retry_exc.retry_count)

        else:

            dateretry = datetime.datetime.utcnow() + datetime.timedelta(seconds=retry_exc.delay)
            updates = {
                "dateretry": dateretry,
                "queue": retry_exc.queue,
                "retry_count": retry_exc.retry_count
            }

            self._save_status("retry", updates, exception=True)

    def save_success(self, result=None):

        dateexpires = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.result_ttl)
        updates = {
            "dateexpires": dateexpires
        }
        if result is not None:
            updates["result"] = result
        if "progress" in self.data:
            updates["progress"] = 1

        self._save_status("success", updates)

    def save_cancel(self):

        dateexpires = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.result_ttl)
        updates = {
            "dateexpires": dateexpires
        }

        self._save_status("cancel", updates)

    def _save_status(self, status, updates=None, exception=False, w=1):

        if self.id is None:
            return

        now = datetime.datetime.utcnow()
        db_updates = {
            "status": status,
            "dateupdated": now
        }
        db_updates.update(updates or {})

        if self.datestarted:
            db_updates["totaltime"] = (now - self.datestarted).total_seconds()

        if context.get_current_config().get("trace_greenlets"):
            current_greenlet = gevent.getcurrent()

            # TODO are we sure the current job is doing the save_status() on itself?
            if hasattr(current_greenlet, "_trace_time"):
                # pylint: disable=protected-access
                db_updates["time"] = current_greenlet._trace_time
                db_updates["switches"] = current_greenlet._trace_switches

        if exception:
            trace = traceback.format_exc()
            context.log.error(trace)
            db_updates["traceback"] = trace
            db_updates["exceptiontype"] = sys.exc_info()[0].__name__

        self.collection.update({
            "_id": self.id
        }, {"$set": db_updates}, w=w, manipulate=False)

        context.metric("jobs.status.%s" % status)

        if self.data:
            self.data.update(db_updates)

    def set_current_io(self, io_data):

        # pylint: disable=protected-access

        if io_data is None:
            if not self._current_io:
                return

            t = time.time() - self._current_io["started"]
            if self.worker:
                self.worker._traced_io["types"][self._current_io["type"]] += t
                self.worker._traced_io["tasks"][self.data["path"]] += t
                self.worker._traced_io["total"] += t

            self._current_io = None

        else:
            io_data["started"] = time.time()
            self._current_io = io_data

    def trace_memory_start(self):
        """ Starts measuring memory consumption """

        objgraph.show_growth(limit=10)

        gc.collect()
        self._memory_start = self.worker.get_memory()

    def trace_memory_stop(self):
        """ Stops measuring memory consumption """

        objgraph.show_growth(limit=10)

        trace_type = context.get_current_config()["trace_memory_type"]
        if trace_type:

            filename = '%s/%s-%s.png' % (
                context.get_current_config()["trace_memory_output_dir"],
                trace_type,
                self.id)

            objgraph.show_chain(
                objgraph.find_backref_chain(
                    random.choice(
                        objgraph.by_type(trace_type)
                    ),
                    objgraph.is_proper_module
                ),
                filename=filename
            )

        gc.collect()
        self._memory_stop = self.worker.get_memory()

        diff = self._memory_stop - self._memory_start

        context.log.debug("Memory diff for job %s : %s" % (self.id, diff))

        # We need to update it later than the results, we need them off memory
        # already.
        self.collection.update(
            {"_id": self.id},
            {"$set": {
                "memory_diff": diff
            }},
            w=1
        )


def get_job_result(job_id):
    job = Job(job_id)
    job.fetch(full_data={"result": 1, "status": 1, "_id": 0})
    return job.data


def queue_raw_jobs(queue, params_list, **kwargs):
    """ Queue some jobs on a raw queue """

    from .queue import Queue
    queue_obj = Queue(queue)
    queue_obj.enqueue_raw_jobs(params_list, **kwargs)


def queue_job(main_task_path, params, **kwargs):
    """ Queue one job on a regular queue """

    return queue_jobs(main_task_path, [params], **kwargs)[0]


def queue_jobs(main_task_path, params_list, queue=None, batch_size=1000):
    """ Queue multiple jobs on a regular queue """

    if len(params_list) == 0:
        return []

    if queue is None:
        task_def = context.get_current_config().get("tasks", {}).get(main_task_path) or {}
        queue = task_def.get("queue", "default")

    from .queue import Queue
    queue_obj = Queue(queue)

    if queue_obj.is_raw:
        raise Exception("Can't queue regular jobs on a raw queue")

    all_ids = []

    for params_group in group_iter(params_list, n=batch_size):

        context.metric("jobs.status.queued", len(params_group))

        # Insert the job in MongoDB
        job_ids = Job.insert([{
            "path": main_task_path,
            "params": params,
            "queue": queue,
            "status": "queued"
        } for params in params_group], w=1, return_jobs=False)

        # Between these 2 calls, a task can be inserted in MongoDB but not queued in Redis.
        # This is the same as dequeueing a task from Redis and being stopped before updating
        # the "started" flag in MongoDB.
        #
        # These jobs will be collected by mrq.basetasks.cleaning.RequeueLostJobs

        # Insert the job ID in Redis
        queue_obj.enqueue_job_ids([str(x) for x in job_ids])

        all_ids += job_ids

    return all_ids
