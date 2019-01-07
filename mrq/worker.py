from future import standard_library
standard_library.install_aliases()
from future.builtins import str, bytes
from future.utils import iteritems
import gevent
import gevent.pool
import os
import signal
import datetime
import time
import socket
import traceback
import psutil
import sys
import json as json_stdlib
import ujson as json
from bson import ObjectId
from redis.lock import LuaLock
from collections import defaultdict
from mrq.utils import load_class_by_path

from .job import Job
from .exceptions import (TimeoutInterrupt, StopRequested, JobInterrupt, AbortInterrupt,
                         RetryInterrupt, MaxRetriesInterrupt, MaxConcurrencyInterrupt)
from .context import (set_current_worker, set_current_job, get_current_job, get_current_config,
                      connections, enable_greenlet_tracing, run_task, log)
from .queue import Queue
from .utils import MongoJSONEncoder, MovingAverage
from .processes import Process
from .redishelpers import redis_key


class Worker(Process):
    """ Main worker class """

    # Allow easy overloading
    job_class = Job

    # See the doc for valid statuses
    status = "init"

    mongodb_jobs = None
    mongodb_logs = None
    redis = None

    def __init__(self):

        set_current_worker(self)

        if self.config.get("trace_greenlets"):
            enable_greenlet_tracing()

        self.datestarted = datetime.datetime.utcnow()

        self.done_jobs = 0
        self.max_jobs = self.config["max_jobs"]
        self.max_time = datetime.timedelta(seconds=self.config["max_time"]) or None

        self.paused_queues = set()

        self.connected = False  # MongoDB + Redis

        self.process = psutil.Process(os.getpid())
        self.greenlet = gevent.getcurrent()
        self.graceful_stop = None

        self.work_lock = gevent.lock.Semaphore()

        if self.config.get("worker_id"):
            self.id = ObjectId(self.config["worker_id"])
        else:
            self.id = ObjectId()

        if self.config.get("name"):
            self.name = self.config["name"]
        else:
            # Generate a somewhat human-readable name for this worker
            self.name = "%s.%s" % (socket.gethostname().split(".")[0], os.getpid())

        self.pool_size = self.config["greenlets"]
        self.pool_usage_average = MovingAverage((60 / self.config["report_interval"] or 1))

        self.set_logger()

        self.refresh_queues(fatal=True)
        self.queues_with_notify = list({redis_key("notify", q) for q in self.queues if q.use_notify()})
        self.has_subqueues = any([queue.endswith("/") for queue in self.config["queues"]])

        self.log.info(
            "Starting worker on %s queues with %s greenlets" %
            (len(self.queues), self.pool_size)
        )

        self.gevent_pool = gevent.pool.Pool(self.pool_size)

        # Keep references to main greenlets
        self.greenlets = {}

        # TODO by "tag"?
        self._traced_io = {
            "types": defaultdict(float),
            "tasks": defaultdict(float),
            "total": 0
        }

        if self.config["ensure_indexes"]:
            run_task("mrq.basetasks.indexes.EnsureIndexes", {})

    def set_logger(self):
        import logging

        self.log = logging.getLogger(str(self.id))
        logging.basicConfig(format=self.config["log_format"])
        self.log.setLevel(getattr(logging, self.config["log_level"]))
        # No need to send worker logs to mongo?
        # logger_class = load_class_by_path(self.config["logger"])

        # # All mrq handlers must have worker and collection keyword arguments
        # if self.config["logger"].startswith("mrq"):
        #     self.log_handler = logger_class(collection=self.config["mongodb_logs"], worker=str(self.id), **self.config["logger_config"])
        # else:
        #     self.log_handler = logger_class(**self.config["logger_config"])
        # self.log.addHandler(self.log_handler)

    @property
    def config(self):
        return get_current_config()

    def connect(self, force=False):

        if self.connected and not force:
            return

        # Accessing connections attributes will automatically connect
        self.redis = connections.redis
        self.mongodb_jobs = connections.mongodb_jobs
        self.mongodb_logs = connections.mongodb_logs

        self.connected = True

    def greenlet_scheduler(self):

        redis_scheduler_lock_key = "%s:schedulerlock" % get_current_config()["redis_prefix"]
        while True:
            with LuaLock(connections.redis, redis_scheduler_lock_key,
                         timeout=self.config["scheduler_interval"] + 10, blocking=False, thread_local=False):
                self.scheduler.check()

            time.sleep(self.config["scheduler_interval"])

    def greenlet_report(self):
        """ This greenlet always runs in background to update current status
            in MongoDB every N seconds.

            Caution: it might get delayed when doing long blocking operations.
            Should we do this in a thread instead?
         """

        self.report_worker(w=1)
        while True:
            try:
                self.report_worker()
            except Exception as e:  # pylint: disable=broad-except
                self.log.error("When reporting: %s" % e)
            finally:
                time.sleep(self.config["report_interval"])

    def greenlet_logs(self):
        """ This greenlet always runs in background to update current
            logs in MongoDB every 10 seconds.

            Caution: it might get delayed when doing long blocking operations.
            Should we do this in a thread instead?
         """

        while True:
            try:
                self.flush_logs()
            except Exception as e:  # pylint: disable=broad-except
                self.log.error("When flushing logs: %s" % e)
            finally:
                time.sleep(self.config["report_interval"])

    def greenlet_subqueues(self):

        while True:
            self.refresh_queues()
            time.sleep(self.config["subqueues_refresh_interval"])

    def refresh_queues(self, fatal=False):
        """ Updates the list of currently known queues and subqueues """

        try:
            queues = []
            prefixes = [q for q in self.config["queues"] if q.endswith("/")]
            known_subqueues = Queue.all_known(prefixes=prefixes)

            for q in self.config["queues"]:
                queues.append(Queue(q))
                if q.endswith("/"):
                    for subqueue in known_subqueues:
                        if subqueue.startswith(q):
                            queues.append(Queue(subqueue))

            self.queues = queues

        except Exception as e:  # pylint: disable=broad-except
            self.log.error("When refreshing subqueues: %s", e)
            if fatal:
                raise

    def get_paused_queues(self):
        """ Returns the set of currently paused queues """
        return {q.decode("utf-8") for q in self.redis.smembers(redis_key("paused_queues"))}

    def greenlet_paused_queues(self):

      while True:

          # Update the process-local list of paused queues
          self.paused_queues = self.get_paused_queues()
          time.sleep(self.config["paused_queues_refresh_interval"])

    def get_memory(self):
        mmaps = self.process.memory_maps()
        mem = {
            "rss": sum([x.rss for x in mmaps]),
            "swap": sum([getattr(x, 'swap', getattr(x, 'swapped', 0)) for x in mmaps])
        }
        mem["total"] = mem["rss"] + mem["swap"]
        return mem

    def get_worker_report(self, with_memory=False):
        """ Returns a dict containing all the data we can about the current status of the worker and
            its jobs. """

        greenlets = []
        for greenlet in list(self.gevent_pool):
            g = {}
            short_stack = []
            stack = traceback.format_stack(greenlet.gr_frame)
            for s in stack[1:]:
                if "/gevent/hub.py" in s:
                    break
                short_stack.append(s)
            g["stack"] = short_stack

            job = get_current_job(id(greenlet))
            if job:
                job.save()
                if job.data:
                    g["path"] = job.data["path"]
                g["datestarted"] = job.datestarted
                g["id"] = str(job.id)
                g["time"] = getattr(greenlet, "_trace_time", 0)
                g["switches"] = getattr(greenlet, "_trace_switches", None)

                # pylint: disable=protected-access
                if job._current_io is not None:
                    g["io"] = job._current_io

            greenlets.append(g)

        # When faking network latency, all sockets are affected, including OS ones, but
        # we still want reliable reports so this is disabled.
        if (not with_memory) or (self.config["add_network_latency"] != "0" and self.config["add_network_latency"]):
            cpu = {
                "user": 0,
                "system": 0,
                "percent": 0
            }
            mem = {"rss": 0, "swap": 0, "total": 0}
        else:
            cpu_times = self.process.cpu_times()
            cpu = {
                "user": cpu_times.user,
                "system": cpu_times.system,
                "percent": self.process.cpu_percent(0)
            }
            mem = self.get_memory()

        # Avoid sharing passwords or sensitive config!
        whitelisted_config = [
            "max_jobs",
            "max_memory"
            "greenlets",
            "processes",
            "queues",
            "dequeue_strategy",
            "scheduler",
            "name",
            "local_ip",
            "external_ip",
            "agent_id",
            "worker_group"
        ]

        io = None
        if self._traced_io:
            io = {}
            for k, v in iteritems(self._traced_io):
                if k == "total":
                    io[k] = v
                else:
                    io[k] = sorted(list(v.items()), reverse=True, key=lambda x: x[1])

        used_pool_slots = len(self.gevent_pool)
        used_avg = self.pool_usage_average.next(used_pool_slots)

        return {
            "status": self.status,
            "config": {k: v for k, v in iteritems(self.config) if k in whitelisted_config},
            "done_jobs": self.done_jobs,
            "usage_avg": used_avg / self.pool_size,
            "datestarted": self.datestarted,
            "datereported": datetime.datetime.utcnow(),
            "name": self.name,
            "io": io,
            "_id": str(self.id),
            "process": {
                "pid": self.process.pid,
                "cpu": cpu,
                "mem": mem
                # https://code.google.com/p/psutil/wiki/Documentation
                # get_open_files
                # get_connections
                # get_num_ctx_switches
                # get_num_fds
                # get_io_counters
                # get_nice
            },
            "jobs": greenlets
        }

    def report_worker(self, w=0):

        report = self.get_worker_report(with_memory=True)

        if self.config["max_memory"] > 0:
            if report["process"]["mem"]["total"] > (self.config["max_memory"] * 1024 * 1024):
                self.shutdown_max_memory()

        if self.config["report_file"]:
            with open(self.config["report_file"], "wb") as f:
                f.write(bytes(json.dumps(report, ensure_ascii=False), 'utf-8'))  # pylint: disable=no-member

        if "_id" in report:
            del report["_id"]

        try:

            self.mongodb_jobs.mrq_workers.update({
                "_id": ObjectId(self.id)
            }, {"$set": report}, upsert=True, w=w)
        except Exception as e:  # pylint: disable=broad-except
            self.log.debug("Worker report failed: %s" % e)

    def greenlet_timeouts(self):
        """ This greenlet kills jobs in other greenlets if they timeout.
        """

        while True:
            now = datetime.datetime.utcnow()
            for greenlet in list(self.gevent_pool):
                job = get_current_job(id(greenlet))
                if job and job.timeout and job.datestarted:
                    expires = job.datestarted + datetime.timedelta(seconds=job.timeout)
                    if now > expires:
                        job.kill(block=False, reason="timeout")

            time.sleep(1)


    def greenlet_admin(self):
        """ This greenlet is used to get status information about the worker
            when --admin_port was given
        """

        if self.config["processes"] > 1:
            self.log.debug(
                "Admin server disabled because of multiple processes.")
            return

        class Devnull(object):
            def write(self, *_):
                pass

        from gevent import pywsgi

        def admin_routes(env, start_response):
            path = env["PATH_INFO"]
            status = "200 OK"
            res = ""
            if path in ["/", "/report", "/report_mem"]:
                report = self.get_worker_report(with_memory=(path == "/report_mem"))
                res = bytes(json_stdlib.dumps(report, cls=MongoJSONEncoder), 'utf-8')
            elif path == "/wait_for_idle":
                self.wait_for_idle()
                res = bytes("idle", "utf-8")
            else:
                status = "404 Not Found"
            start_response(status, [('Content-Type', 'application/json')])
            return [res]

        server = pywsgi.WSGIServer((self.config["admin_ip"], self.config["admin_port"]), admin_routes, log=Devnull())

        try:
            self.log.debug("Starting admin server on port %s" % self.config["admin_port"])
            server.serve_forever()
        except Exception as e:  # pylint: disable=broad-except
            self.log.debug("Error in admin server : %s" % e)

    def flush_logs(self):
        for handler in self.log.handlers:
            handler.flush()

    def wait_for_idle(self):
        """ Waits until the worker has nothing more to do. Very useful in tests """

        # Be mindful that this is being executed in a different greenlet than the work_* methods.

        while True:

            time.sleep(0.01)

            with self.work_lock:

                if self.status != "wait":
                    continue

                if len(self.gevent_pool) > 0:
                    continue

                # Force a refresh of the current subqueues, one might just have been created.
                self.refresh_queues()

                # We might be dequeueing a new subqueue. Double check that we don't have anything more to do
                outcome, dequeue_jobs = self.work_once(free_pool_slots=1, max_jobs=None)

                if outcome is "wait" and dequeue_jobs == 0:
                    break

    def work(self):
        """Starts the work loop.

        """
        self.work_init()

        self.work_loop(max_jobs=self.max_jobs, max_time=self.max_time)

        self.work_stop()

    def work_init(self):

        self.connect()

        self.status = "started"

        # An interval of 0 disables the refresh
        if self.has_subqueues and self.config["subqueues_refresh_interval"] > 0:
            self.greenlets["subqueues"] = gevent.spawn(self.greenlet_subqueues)

        # An interval of 0 disables the refresh
        if self.config["paused_queues_refresh_interval"] > 0:
            self.greenlets["paused_queues"] = gevent.spawn(self.greenlet_paused_queues)

        if self.config["report_interval"] > 0:
            self.greenlets["report"] = gevent.spawn(self.greenlet_report)
            self.greenlets["logs"] = gevent.spawn(self.greenlet_logs)

        if self.config["admin_port"]:
            self.greenlets["admin"] = gevent.spawn(self.greenlet_admin)

        self.greenlets["timeouts"] = gevent.spawn(self.greenlet_timeouts)

        if self.config["scheduler"] and self.config["scheduler_interval"] > 0:

            from .scheduler import Scheduler
            self.scheduler = Scheduler(self.mongodb_jobs.mrq_scheduled_jobs, self.config.get("scheduler_tasks") or [])

            self.scheduler.check_config_integrity()  # If this fails, we won't dequeue any jobs

            self.greenlets["scheduler"] = gevent.spawn(self.greenlet_scheduler)

        self.install_signal_handlers()

    def work_loop(self, max_jobs=None, max_time=None):

        self.done_jobs = 0
        self.datestarted_work_loop = datetime.datetime.utcnow()
        self.queue_offset = 0

        try:

            max_time_reached = False

            while True:

                if self.graceful_stop:
                    break

                # If the scheduler greenlet is crashed, fail loudly.
                if self.config["scheduler"] and not self.greenlets["scheduler"]:
                    self.exitcode = 1
                    break

                while True:

                    # we put this here to make sure we have a strict limit on max_time
                    if max_time and datetime.datetime.utcnow() - self.datestarted >= max_time:
                        self.log.info("Reached max_time=%s" % max_time.seconds)
                        max_time_reached = True
                        break

                    free_pool_slots = self.gevent_pool.free_count()

                    if max_jobs:
                        total_started = (self.pool_size - free_pool_slots) + self.done_jobs
                        free_pool_slots = min(free_pool_slots, max_jobs - total_started)
                        if free_pool_slots == 0:
                            break

                    if free_pool_slots > 0:
                        break

                    self.status = "full"
                    self.gevent_pool.wait_available(timeout=60)

                if max_time_reached:
                    break

                self.status = "spawn"
                with self.work_lock:
                    outcome, dequeue_jobs = self.work_once(free_pool_slots=free_pool_slots, max_jobs=max_jobs)
                self.status = "wait"

                if outcome == "break":
                    break

                if outcome == "wait":
                    self.work_wait()

        except StopRequested:
            pass

        finally:

            try:

                self.log.debug("Joining the greenlet pool...")
                self.status = "join"

                self.gevent_pool.join(timeout=None, raise_error=False)
                self.log.debug("Joined.")

            except StopRequested:
                pass

        self.datestopped_work_loop = datetime.datetime.utcnow()
        lifetime = self.datestopped_work_loop - self.datestarted_work_loop
        job_rate = float(self.done_jobs) / lifetime.total_seconds()
        self.log.info("Worker spent %.3f seconds performing %s jobs (%.3f jobs/second)" % (
            lifetime.total_seconds(), self.done_jobs, job_rate
        ))

    def work_once(self, free_pool_slots=1, max_jobs=None):
        """ Does one lookup for new jobs, inside the inner work loop """

        dequeued_jobs = 0

        available_queues = [
            queue for queue in self.queues
            if queue.root_id not in self.paused_queues and
            queue.id not in self.paused_queues
        ]

        for queue_i in range(len(available_queues)):

            queue = available_queues[(queue_i + self.queue_offset) % len(available_queues)]

            max_jobs_per_queue = free_pool_slots - dequeued_jobs

            if max_jobs_per_queue <= 0:
                queue_i -= 1
                break

            if self.config["dequeue_strategy"] == "parallel":
                max_jobs_per_queue = max(1, int(max_jobs_per_queue / (len(available_queues) - queue_i)))

            for job in queue.dequeue_jobs(
                max_jobs=max_jobs_per_queue,
                job_class=self.job_class,
                worker=self
            ):
                dequeued_jobs += 1

                self.gevent_pool.spawn(self.perform_job, job)

        # At the next pass, start at the next queue to avoid always dequeuing the same one
        if self.config["dequeue_strategy"] == "parallel":
            self.queue_offset = (self.queue_offset + queue_i + 1) % len(self.queues)

        # TODO consider this when dequeuing jobs to have strict limits
        if max_jobs and self.done_jobs >= max_jobs:
            self.log.info("Reached max_jobs=%s" % self.done_jobs)
            return "break", dequeued_jobs

        # We seem to have exhausted available jobs, we can sleep for a
        # while.
        if dequeued_jobs == 0:

            if self.config["dequeue_strategy"] == "burst":
                self.log.info("Burst mode: stopping now because queues were empty")
                return "break", dequeued_jobs

            return "wait", dequeued_jobs

        return None, dequeued_jobs

    def work_wait(self):
        """ Wait for new jobs to arrive """

        if len(self.queues_with_notify) > 0:
            # https://github.com/antirez/redis/issues/874
            connections.redis.blpop(*(self.queues_with_notify + [max(1, int(self.config["max_latency"]))]))
        else:
            gevent.sleep(self.config["max_latency"])

    def work_stop(self):

        self.status = "kill"

        self.gevent_pool.kill(exception=JobInterrupt, block=True)

        for g in self.greenlets:
            g_time = getattr(self.greenlets[g], "_trace_time", 0)
            g_switches = getattr(self.greenlets[g], "_trace_switches", None)
            self.greenlets[g].kill(block=True)
            self.log.debug(
                "Greenlet for %s killed (%0.5fs, %s switches)." %
                (g, g_time, g_switches))

        self.status = "stop"

        self.report_worker(w=1)
        self.flush_logs()

        g_time = getattr(self.greenlet, "_trace_time", 0)
        g_switches = getattr(self.greenlet, "_trace_switches", None)
        self.log.debug(
            "Exiting main worker greenlet (%0.5fs, %s switches)." %
            (g_time, g_switches))

    def perform_job(self, job):
        """ Wraps a job.perform() call with timeout logic and exception handlers.

            This is the first call happening inside the greenlet.
        """

        if self.config["trace_memory"]:
            job.trace_memory_start()

        set_current_job(job)

        try:
            job.perform()

        except MaxConcurrencyInterrupt:
            self.log.error("Max concurrency reached")
            job._save_status("maxconcurrency", exception=True)

        except RetryInterrupt:
            self.log.error("Caught retry")
            job.save_retry(sys.exc_info()[1])

        except MaxRetriesInterrupt:
            self.log.error("Max retries reached")
            job._save_status("maxretries", exception=True)

        except AbortInterrupt:
            self.log.error("Caught abort")
            job.save_abort()

        except TimeoutInterrupt:
            self.log.error("Job timeouted after %s seconds" % job.timeout)
            job._save_status("timeout", exception=True)

        except JobInterrupt:
            self.log.error("Job interrupted")
            job._save_status("interrupt", exception=True)

        except Exception:
            self.log.error("Job failed")
            job._save_status("failed", exception=True)

        finally:

            set_current_job(None)

            self.done_jobs += 1

            if self.config["trace_memory"]:
                job.trace_memory_stop()

    def shutdown_graceful(self):
        """ Graceful shutdown: waits for all the jobs to finish. """

        self.log.info("Graceful shutdown...")
        raise StopRequested()  # pylint: disable=nonstandard-exception

    def shutdown_max_memory(self):

        self.log.info("Max memory reached, shutdown...")
        self.graceful_stop = True

    def shutdown_now(self):
        """ Forced shutdown: interrupts all the jobs. """

        self.log.info("Forced shutdown...")
        self.status = "killing"

        self.gevent_pool.kill(exception=JobInterrupt, block=False)

        raise StopRequested()  # pylint: disable=nonstandard-exception
