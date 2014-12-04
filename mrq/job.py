import datetime
from bson import ObjectId
import pymongo
import time
from .exceptions import RetryInterrupt, CancelInterrupt
from .utils import load_class_by_path
from .queue import Queue
from .context import get_current_worker, log, connections, get_current_config, set_current_job, metric
import gevent
import objgraph
import random
import gc
from collections import defaultdict
import traceback
import sys


class Job(object):

    # Seconds the job can last before timeouting
    timeout = 300

    # Seconds the results are kept in MongoDB
    result_ttl = 7 * 24 * 3600

    progress = 0

    # Exceptions that don't mark the task as failed but as retry
    retry_on_exceptions = (
        pymongo.errors.AutoReconnect,
        pymongo.errors.OperationFailure,
        pymongo.errors.ConnectionFailure,
        RetryInterrupt
    )

    # Exceptions that will make the task as cancelled
    cancel_on_exceptions = (
        CancelInterrupt
    )

    _memory_stop = 0
    _memory_start = 0

    _current_io = None

    def __init__(self, job_id, queue=None, start=False, fetch=False):
        self.worker = get_current_worker()
        self.queue = queue
        self.datestarted = None

        self.collection = connections.mongodb_jobs.mrq_jobs

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
                "status": 1
            }

        if start:

            self.datestarted = datetime.datetime.utcnow()
            self.set_data(self.collection.find_and_modify(
                {
                    "_id": self.id,
                    "status": {"$nin": ["cancel"]}
                },
                {"$set": {
                    "status": "started",
                    "datestarted": self.datestarted,
                    "worker": self.worker.id
                }},
                fields=fields)
            )

            metric("jobs.status.started")

        else:
            self.set_data(self.collection.find_one({
                "_id": self.id
            }, fields=fields))

        if self.data is None:
            log.info(
                "Job %s not found in MongoDB or status was cancelled!" %
                self.id)

        return self

    def set_data(self, data):
        self.data = data
        if self.data is None:
            return

        if "path" in self.data:
            task_def = get_current_config().get("tasks", {}).get(
                self.data["path"]
            ) or {}
            self.timeout = task_def.get("timeout", self.timeout)
            self.result_ttl = task_def.get("result_ttl", self.result_ttl)

    def set_progress(self, ratio, save=False):
        self.data["progress"] = ratio
        self.saved = False

        # If not saved, will be updated in the next worker report
        if save:
            self.save()

    def save(self):
        """ Will be called at each worker report. """

        if not self.saved and self.data and "progress" in self.data:
            # TODO should we save more fields?
            self.collection.update({"_id": self.id}, {"$set": {
                "progress": self.data["progress"]
            }})
            self.saved = True

    @classmethod
    def insert(self, jobs_data, queue=None, return_jobs=True, w=1):
        """ Insert a job into MongoDB """

        now = datetime.datetime.utcnow()
        for data in jobs_data:
            if data["status"] == "started":
                data["datestarted"] = now

        inserted = connections.mongodb_jobs.mrq_jobs.insert(
            jobs_data,
            manipulate=True,
            w=w
        )

        if return_jobs:
            jobs = []
            for data in jobs_data:
                job = self(data["_id"], queue=queue)
                job.set_data(data)
                if data["status"] == "started":
                    job.datestarted = data["datestarted"]
                jobs.append(job)

            return jobs
        else:
            return inserted

    def save_status(
            self,
            status,
            result=None,
            exception=False,
            dateretry=None,
            queue=None,
            w=1):

        if self.id is None:
            return

        now = datetime.datetime.utcnow()
        updates = {
            "status": status,
            "dateupdated": now
        }

        if self.datestarted:
            updates["totaltime"] = (now - self.datestarted).total_seconds()
        if result is not None:
            updates["result"] = result

        if dateretry is not None:
            updates["dateretry"] = dateretry
        if queue is not None:
            updates["queue"] = queue
        if get_current_config().get("trace_greenlets"):
            current_greenlet = gevent.getcurrent()

            # TODO are we sure the current job is doing the save_status() on itself?
            if hasattr(current_greenlet, "_trace_time"):
                updates["time"] = current_greenlet._trace_time
                updates["switches"] = current_greenlet._trace_switches

        if exception:
            trace = traceback.format_exc()
            log.error(trace)
            updates["traceback"] = trace
            updates["exceptiontype"] = sys.exc_info()[0].__name__

        # Make the job document expire
        if status in ("success", "cancel"):
            updates["dateexpires"] = now + \
                datetime.timedelta(seconds=self.result_ttl)

        if status == "success" and "progress" in self.data:
            updates["progress"] = 1

        self.collection.update({
            "_id": self.id
        }, {"$set": updates}, w=w, manipulate=False)

        metric("jobs.status.%s" % status)

        if self.data:
            self.data.update(updates)

    def save_retry(self, exc, exception=False):

        countdown = 24 * 3600

        if isinstance(exc, RetryInterrupt) and exc.countdown is not None:
            countdown = exc.countdown

        queue = None
        if isinstance(exc, RetryInterrupt) and exc.queue:
            queue = exc.queue

        if countdown == 0:
            self.requeue(queue=queue)
        else:
            now = datetime.datetime.utcnow()
            self.save_status(
                "retry",
                exception=exception,
                dateretry=now + datetime.timedelta(seconds=countdown),
                queue=queue
            )

    def retry(self, queue=None, countdown=None, max_retries=None):

        if self.task.cancel_on_retry:
            raise CancelInterrupt()
        else:
            exc = RetryInterrupt()
            exc.queue = queue
            exc.countdown = countdown
            raise exc

    def cancel(self):
        self.save_status("cancel")

    def requeue(self, queue=None):

        if not queue:
            if not self.data or not self.data.get("queue"):
                self.fetch(full_data={"_id": 0, "queue": 1, "path": 1})
            queue = self.data["queue"]

        queue_obj = Queue(queue)

        self.save_status("queued", queue=queue)

        # Between these two lines, jobs can become "lost" too.

        queue_obj.enqueue_job_ids([str(self.id)])

    def perform(self):
        """ Loads and starts the main task for this job, the saves the result. """

        if self.data is None:
            return

        log.debug("Starting %s(%s)" % (self.data["path"], self.data["params"]))
        task_class = load_class_by_path(self.data["path"])

        self.task = task_class()

        self.task.is_main_task = True

        result = self.task.run(self.data["params"])

        self.save_status("success", result)

        if get_current_config().get("trace_greenlets"):

            # TODO: this is not the exact greenlet_time measurement because it doesn't
            # take into account the last switch's time. This is why we force a last switch.
            # This does cause a performance overhead. Instead, we should print the
            # last timing directly from the trace() function in context?

            gevent.sleep(0)
            current_greenlet = gevent.getcurrent()
            t = (datetime.datetime.utcnow() - self.datestarted).total_seconds()
            log.debug(
                "Job %s success: %0.6fs total, %0.6fs in greenlet, %s switches" %
                (self.id,
                 t,
                 current_greenlet._trace_time,
                 current_greenlet._trace_switches - 1)
            )

        else:
            log.debug("Job %s success: %0.6fs total" % (
                self.id, (datetime.datetime.utcnow() -
                          self.datestarted).total_seconds()
            ))

        return result

    def wait(self, poll_interval=1, timeout=None, full_data=False):
        """ Wait for this job to finish. """

        end_time = None
        if timeout:
            end_time = time.time() + timeout

        while (end_time is None or time.time() < end_time):

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

    def set_current_io(self, io_data):
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

    def subpool_map(self, *args, **kwargs):
        """ Deprecated! Use from mrq.context """
        from mrq.context import subpool_map
        return subpool_map(*args, **kwargs)

    def trace_memory_stop(self):
        """ Stops measuring memory consumption """

        objgraph.show_growth(limit=10)

        trace_type = get_current_config()["trace_memory_type"]
        if trace_type:

            filename = '%s/%s-%s.png' % (
                get_current_config()["trace_memory_output_dir"],
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

        log.debug("Memory diff for job %s : %s" % (self.id, diff))

        # We need to update it later than the results, we need them off memory
        # already.
        self.collection.update(
            {"_id": self.id},
            {"$set": {
                "memory_diff": diff
            }},
            w=1
        )
