from future import standard_library
standard_library.install_aliases()
from future.builtins import str, object
import datetime
from bson import ObjectId
from redis.exceptions import LockError
import time
from .exceptions import RetryInterrupt, MaxRetriesInterrupt, AbortInterrupt, MaxConcurrencyInterrupt
from .utils import load_class_by_path, group_iter
import gevent
import objgraph
import random
import gc
from collections import defaultdict
import traceback
import sys
import urllib.parse
import re
import linecache
import fnmatch
import encodings
import copyreg
from . import context
from pymongo import DESCENDING


FINAL_STATUSES = {"timeout", "abort", "failed", "success", "interrupt", "retry", "maxretries", "maxconcurrency", "delayed"}
TRANSIENT_STATUSES = {"cancel", "queued", "started"}


class Job(object):

    timeout = None
    result_ttl = None
    abort_ttl = None
    cancel_ttl = None
    max_retries = None
    retry_delay = None

    # All values above can be overrided from the TASKS config

    progress = 0

    _memory_stop = 0
    _memory_start = 0

    _current_io = None

    # Has this job been inserted in MongoDB yet?
    stored = None

    # List of statuses that don't trigger a storage of this task in MongoDB on raw queues.
    # In task config, use ("started", "success") to avoid storing successful raw tasks at all
    statuses_no_storage = None

    def __init__(self, job_id, queue=None, start=False, fetch=False):
        self.worker = context.get_current_worker()
        self.queue = queue
        self.datestarted = None

        self.collection = context.connections.mongodb_jobs.mrq_jobs

        if job_id is None:
            self.id = None
        else:
            if isinstance(job_id, bytes):
                self.id = ObjectId(job_id.decode('utf-8'))
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

    @property
    def redis_max_concurrency_key(self):
        """ Returns the global redis key used to control job concurrency """
        return "%s:c:%s" % (context.get_current_config()["redis_prefix"], self.data["path"])

    def exists(self):
        """ Returns True if a job with the current _id exists in MongoDB. """
        return bool(self.collection.find_one({"_id": self.id}, projection={"_id": 1}))

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
                "retry_count": 1,
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
                },
                "$unset": {
                    "dateexpires": 1 # we don't want started jobs to expire unexpectedly
                }},
                projection=fields)
            )

            context.metric("jobs.status.started")

        else:
            self.set_data(self.collection.find_one({
                "_id": self.id
            }, projection=fields))

        if self.data is None:
            context.log.info(
                "Job %s not found in MongoDB or status was cancelled!" %
                self.id)

        self.stored = True

        return self

    def get_task_config(self):
        cfg = context.get_current_config()
        return cfg.get("tasks", {}).get(
            self.data["path"]
        ) or {}

    def set_data(self, data):
        self.data = data
        if self.data is None:
            return

        if "path" in self.data:
            cfg = context.get_current_config()
            task_def = self.get_task_config()

            self.timeout = task_def.get("timeout", cfg["default_job_timeout"])
            self.default_ttl = task_def.get("default_ttl", cfg["default_job_ttl"])
            self.result_ttl = task_def.get("result_ttl", cfg["default_job_result_ttl"]) # success ttl
            self.abort_ttl = task_def.get("abort_ttl", cfg["default_job_abort_ttl"])
            self.cancel_ttl = task_def.get("cancel_ttl", cfg["default_job_cancel_ttl"])
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
    def insert(cls, jobs_data, queue=None, statuses_no_storage=None, return_jobs=True, w=None, j=None):
        """ Insert a job into MongoDB """

        now = datetime.datetime.utcnow()
        for data in jobs_data:
            if data["status"] == "started":
                data["datestarted"] = now

        no_storage = (statuses_no_storage is not None) and ("started" in statuses_no_storage)
        if no_storage and return_jobs:
            for data in jobs_data:
                data["_id"] = ObjectId()  # Give the job a temporary ID
        else:
            inserted = context.connections.mongodb_jobs.mrq_jobs.insert(
                jobs_data,
                manipulate=True,
                w=w,
                j=j
            )

        if return_jobs:
            jobs = []
            for data in jobs_data:
                job = cls(data["_id"], queue=queue)
                job.set_data(data)
                job.statuses_no_storage = statuses_no_storage
                job.stored = (not no_storage)
                if data["status"] == "started":
                    job.datestarted = data["datestarted"]
                jobs.append(job)

            return jobs
        else:
            return inserted

    def _attach_original_exception(self, exc):
        """ Often, a retry will be raised inside an "except" block.
            This Keep track of the first exception for debugging purposes """

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

        exc.queue = queue or self.queue or self.data.get("queue") or "default"
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

        self._save_status("queued", updates={
            "queue": queue,
            "datequeued": datetime.datetime.utcnow(),
            "retry_count": retry_count
        })

    def perform(self):
        """ Loads and starts the main task for this job, the saves the result. """

        if self.data is None:
            return

        context.log.debug("Starting %s(%s)" % (self.data["path"], self.data["params"]))
        task_class = load_class_by_path(self.data["path"])

        self.task = task_class()

        self.task.is_main_task = True

        if not self.task.max_concurrency:

            result = self.task.run_wrapped(self.data["params"])

        else:

            if self.task.max_concurrency > 1:
                raise NotImplementedError()

            lock = None
            try:

                # TODO: implement a semaphore
                lock = context.connections.redis.lock(self.redis_max_concurrency_key, timeout=self.timeout + 5)
                if not lock.acquire(blocking=True, blocking_timeout=0):
                    raise MaxConcurrencyInterrupt()

                result = self.task.run_wrapped(self.data["params"])

            finally:
                try:
                    if lock:
                        lock.release()
                except LockError:
                    pass

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
            }, projection=({
                "_id": 0,
                "result": 1,
                "status": 1
            } if not full_data else None))

            if job_data:
                return job_data

            time.sleep(poll_interval)
        raise Exception("Waited for job result for %s seconds, timeout." % timeout)

    def kill(self, block=False, reason="unknown"):
        """ Forcefully kill all greenlets associated with this job """

        current_greenletid = id(gevent.getcurrent())

        trace = "Job killed: %s" % reason
        for greenlet, job in context._GLOBAL_CONTEXT["greenlets"].values():
            greenletid = id(greenlet)
            if job and job.id == self.id and greenletid != current_greenletid:
                greenlet.kill(block=block)
                trace += "\n\n--- Greenlet %s ---\n" % greenletid
                trace += "".join(traceback.format_stack(greenlet.gr_frame))
            context._GLOBAL_CONTEXT["greenlets"].pop(greenletid, None)

        if reason == "timeout" and self.data["status"] != "timeout":
            updates = {
                "exceptiontype": "TimeoutInterrupt",
                "traceback": trace
            }
            self._save_status("timeout", updates=updates, exception=False)

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

    def _save_traceback_history(self, status, trace, job_exc):
        """ Create traceback history or add a new traceback to history. """
        failure_date = datetime.datetime.utcnow()

        new_history = {
            "date": failure_date,
            "status": status,
            "exceptiontype": job_exc.__name__
        }

        traces = trace.split("---- Original exception: -----")
        if len(traces) > 1:
            new_history["original_traceback"] = traces[1]
        worker = context.get_current_worker()
        if worker:
            new_history["worker"] = worker.id
        new_history["traceback"] = traces[0]
        self.collection.update({
            "_id": self.id
        }, {"$push": {"traceback_history": new_history}})

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

        dateexpires = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.cancel_ttl)
        updates = {
            "dateexpires": dateexpires
        }

        self._save_status("cancel", updates)

    def save_abort(self):
        dateexpires = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.abort_ttl)
        updates = {
            "dateexpires": dateexpires
        }

        self._save_status("abort", updates, exception=True)

    def _save_status(self, status, updates=None, exception=False, w=None, j=None):

        if self.id is None:
            return

        # Forbid some status transitions
        if self.data and self.data.get("status") in FINAL_STATUSES and status not in TRANSIENT_STATUSES:
            context.log.error("Can't go from status %s to %s" % (self.data["status"], status))
            return

        context.metric("jobs.status.%s" % status)

        if self.stored is False and self.statuses_no_storage is not None and status in self.statuses_no_storage:
            return

        now = datetime.datetime.utcnow()
        db_updates = {
            "status": status,
            "dateupdated": now
        }

        # we don't want started jobs to expire unexpectedly
        if status not in ["started", "success", "abort", "cancel"] and hasattr(self, "default_ttl") and self.default_ttl is not None:
            db_updates["dateexpires"] = (self.data.get("datequeued") or now) + datetime.timedelta(seconds=self.default_ttl)

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
            exc, value = sys.exc_info()[0:2]
            if hasattr(value, "subpool_traceback"):
                trace = "Exception first caught in a subpool. Traceback:\n%s\n%s" % (value.subpool_traceback, trace)
            db_updates["traceback"] = trace
            db_updates["exceptiontype"] = exc.__name__

        if self.data:
            self.data.update(db_updates)
            # get all data before updating them
            current_queue = (db_updates or {}).get("queue") or self.data["queue"]
            old_queue = self.data.get("queue")
            old_status = self.data.get("status")
            raw_queue = self.data.get("raw_queue")
            retry_count = self.data.get("retry_count", 0)

        # In the most common case, we allow an optimization on Mongo writes
        if status == "success":
            if w is None:
                w = getattr(self.task, "status_success_update_w", None)
            if j is None:
                j = getattr(self.task, "status_success_update_j", None)

        # This job wasn't inserted because "started" is in statuses_no_storage
        # So we must insert it for the first time instead of updating it.
        if self.stored is False:
            db_updates["queue"] = self.data["queue"]
            db_updates["params"] = self.data["params"]
            db_updates["path"] = self.data["path"]
            self.collection.insert(db_updates, w=w, j=j, manipulate=True)
            self.id = db_updates["_id"]  # Persistent ID assigned by the server
            self.stored = True

        else:
            self.collection.update({
                "_id": self.id
            }, {"$set": db_updates}, w=w, j=j, manipulate=False)

        if exception:
            self._save_traceback_history(status, trace, exc)

        if self.data:
            with context.connections.redis.pipeline(transaction=False) as pipe:
                if status != "started":
                    # Queue change
                    if current_queue != old_queue:
                        pipe.decr("queuesize:%s" % old_queue)
                        if status == "queued":
                            pipe.incr("queuesize:%s" % current_queue)

                    # Regular queues
                    elif status == "queued" and old_status != "started":
                        pipe.incr("queuesize:%s" % current_queue)

                    elif status != "queued" and not raw_queue:
                        pipe.decr("queuesize:%s" % current_queue)

                    # Raw queues retries
                    elif (db_updates or {}).get("retry_count", 0) > retry_count:
                        pipe.incr("queuesize:%s" % current_queue)

                    pipe.expire("queuesize:%s" % current_queue, context.get_current_config().get("queue_ttl"))
                pipe.execute()

    def set_current_io(self, io_data):

        # pylint: disable=protected-access

        if io_data is None:
            if not self._current_io:
                return

            t = time.time() - self._current_io["started"]
            if self.worker and self.data.get("path"):
                self.worker._traced_io["types"][self._current_io["type"]] += t
                self.worker._traced_io["tasks"][self.data["path"]] += t
                self.worker._traced_io["total"] += t

            self._current_io = None

        else:
            io_data["started"] = time.time()
            self._current_io = io_data

    def trace_memory_clean_caches(self):
        """ Avoid polluting results with some builtin python caches """

        urllib.parse.clear_cache()
        re.purge()
        linecache.clearcache()
        copyreg.clear_extension_cache()

        if hasattr(fnmatch, "purge"):
            fnmatch.purge()  # pylint: disable=no-member
        elif hasattr(fnmatch, "_purge"):
            fnmatch._purge()  # pylint: disable=no-member

        if hasattr(encodings, "_cache") and len(encodings._cache) > 0:
            encodings._cache = {}

        for handler in context.log.handlers:
            handler.flush()

    def trace_memory_start(self):
        """ Starts measuring memory consumption """

        self.trace_memory_clean_caches()

        objgraph.show_growth(limit=30)

        gc.collect()
        self._memory_start = self.worker.get_memory()["total"]

    def trace_memory_stop(self):
        """ Stops measuring memory consumption """

        self.trace_memory_clean_caches()

        objgraph.show_growth(limit=30)

        trace_type = context.get_current_config()["trace_memory_type"]
        if trace_type:

            filename = '%s/%s-%s.png' % (
                context.get_current_config()["trace_memory_output_dir"],
                trace_type,
                self.id)

            chain = objgraph.find_backref_chain(
                random.choice(
                    objgraph.by_type(trace_type)
                ),
                objgraph.is_proper_module
            )
            objgraph.show_chain(chain, filename=filename)
            del filename
            del chain

        gc.collect()
        self._memory_stop = self.worker.get_memory()["total"]

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

def get_latest_job_with_query(query):
        jobs = context.connections.mongodb_jobs.mrq_jobs
        task = jobs.find_one(query, sort=[('datequeued', DESCENDING)])
        return task

def get_job_result(job_id):
    job = Job(job_id)
    job.fetch(full_data={"result": 1, "status": 1, "_id": 0})
    return job.data

def queue_raw_jobs(queue, params_list, **kwargs):
    """ Queue some jobs on a raw queue """

    from .queue import Queue
    queue_obj = Queue(queue)
    queue_obj.enqueue_raw_jobs(params_list, **kwargs)
    # No need to store queue size as we already have a fast way to get raw queue size

def queue_job(main_task_path, params, **kwargs):
    """ Queue one job on a regular queue """

    return queue_jobs(main_task_path, [params], **kwargs)[0]

def set_queues_size(size_by_queues, action="incr"):
    if len(size_by_queues) > 0:
        with context.connections.redis.pipeline(transaction=False) as pipe:
            action_func = getattr(pipe, action)
            for queue in size_by_queues:
                action_func("queuesize:%s" % queue, amount=size_by_queues[queue])
                pipe.expire("queuesize:%s" % queue, context.get_current_config().get("queue_ttl"))
            pipe.execute()

def queue_jobs(main_task_path, params_list, delay=0, queue=None, batch_size=1000):
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

        jobs_data = []
        for params in params_group:
            job_data = {
                "path": main_task_path,
                "params": params,
                "queue": queue,
                "datequeued": datetime.datetime.utcnow(),
                "status": "queued"
            }
            if delay and delay > 0:
                dateretry = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay)
                job_data['status'] = 'delayed'
                job_data['dateretry'] = dateretry

            jobs_data.append(job_data)

        # Insert the job in MongoDB
        job_ids = Job.insert(jobs_data, w=1, return_jobs=False)

        all_ids += job_ids

    queue_obj.notify(len(all_ids))
    set_queues_size({queue: len(all_ids)})

    return all_ids
