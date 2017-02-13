from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import bytes
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
import http.server
from bson import ObjectId
from collections import defaultdict

from .job import Job
from .exceptions import (TimeoutInterrupt, StopRequested, JobInterrupt, AbortInterrupt,
                         RetryInterrupt, MaxRetriesInterrupt, MaxConcurrencyInterrupt)
from .context import (set_current_worker, set_current_job, get_current_job, get_current_config,
                      connections, enable_greenlet_tracing)
from .queue import Queue
from .utils import MongoJSONEncoder, MovingAverage


class Worker(object):

    """ Main worker class. """

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

        self.connected = False  # MongoDB + Redis

        self.exitcode = 0
        self.process = psutil.Process(os.getpid())
        self.greenlet = gevent.getcurrent()
        self.graceful_stop = None

        self.idle_event = gevent.event.Event()
        self.idle_wait_count = 0

        self.id = ObjectId()
        if self.config.get("name"):
            self.name = self.config["name"]
        else:
            # Generate a somewhat human-readable name for this worker
            self.name = "%s.%s" % (socket.gethostname().split(".")[0], os.getpid())

        self.pool_size = self.config["greenlets"]
        self.pool_usage_average = MovingAverage((60 / self.config["report_interval"] or 1))

        from .logger import LogHandler
        self.log_handler = LogHandler(quiet=self.config["quiet"])
        self.log = self.log_handler.get_logger(worker=self.id)

        self.queues = [Queue(x, add_to_known_queues=True) for x in self.config["queues"] if x]

        self.log.info(
            "Starting Gevent pool with %s worker greenlets (+ report, logs, adminhttp)" %
            self.pool_size)

        self.gevent_pool = gevent.pool.Pool(self.pool_size)

        # Keep references to main greenlets
        self.greenlets = {}

        # TODO by "tag"?
        self._traced_io = {
            "types": defaultdict(float),
            "tasks": defaultdict(float),
            "total": 0
        }

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

        if self.mongodb_logs:
            self.log_handler.set_collection(self.mongodb_logs.mrq_logs)

        self.connected = True

        # Be mindful that this is done each time a worker starts
        if not self.config["no_mongodb_ensure_indexes"]:
            self.ensure_indexes()

    def ensure_indexes(self):

        if self.mongodb_logs:

            self.mongodb_logs.mrq_logs.ensure_index(
                [("job", 1)], background=False)
            self.mongodb_logs.mrq_logs.ensure_index(
                [("worker", 1)], background=False, sparse=True)

        self.mongodb_jobs.mrq_workers.ensure_index(
            [("status", 1)], background=False)
        self.mongodb_jobs.mrq_workers.ensure_index(
            [("datereported", 1)], background=False, expireAfterSeconds=3600)

        self.mongodb_jobs.mrq_jobs.ensure_index(
            [("status", 1)], background=True)
        self.mongodb_jobs.mrq_jobs.ensure_index(
            [("path", 1)], background=True)
        self.mongodb_jobs.mrq_jobs.ensure_index(
            [("worker", 1)], background=True, sparse=True)
        self.mongodb_jobs.mrq_jobs.ensure_index(
            [("queue", 1)], background=True)
        self.mongodb_jobs.mrq_jobs.ensure_index(
            [("dateexpires", 1)], sparse=True, background=False, expireAfterSeconds=0)
        self.mongodb_jobs.mrq_jobs.ensure_index(
            [("dateretry", 1)], sparse=True, background=False)

        self.mongodb_jobs.mrq_scheduled_jobs.ensure_index(
            [("hash", 1)], unique=True, background=False, drop_dups=True)

        try:
            # This will be default in MongoDB 2.6
            self.mongodb_jobs.command(
                {"collMod": "mrq_jobs", "usePowerOf2Sizes": True})
            self.mongodb_jobs.command(
                {"collMod": "mrq_workers", "usePowerOf2Sizes": True})
        except:  # pylint: disable=bare-except
            pass

    def greenlet_scheduler(self):

        from .scheduler import Scheduler
        scheduler = Scheduler(self.mongodb_jobs.mrq_scheduled_jobs)

        scheduler.sync_tasks(self.config.get("scheduler_tasks") or [])

        while True:
            scheduler.check()
            time.sleep(int(self.config["scheduler_interval"]))

    def greenlet_report(self):
        """ This greenlet always runs in background to update current status
            in MongoDB every 10 seconds.

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
                self.flush_logs(w=0)
            except Exception as e:  # pylint: disable=broad-except
                self.log.error("When flushing logs: %s" % e)
            finally:
                time.sleep(self.config["report_interval"])

    def greenlet_subqueues(self):

        has_subqueues = any([
            queue.endswith(get_current_config().get("subqueues_delimiter"))
            for queue in self.config["queues"]
        ])

        while True:

            # Update the process-local list of known queues
            Queue.known_queues = Queue.redis_known_queues()

            queues = []
            try:
                for queue in self.config["queues"]:
                    if queue.endswith(get_current_config().get("subqueues_delimiter")):
                        queues += Queue(queue, add_to_known_queues=True).redis_known_subqueues()
                    else:
                        queues.append(Queue(queue, add_to_known_queues=True))

            except Exception as e:  # pylint: disable=broad-except
                self.log.error("When refreshing subqueues: %s", e)
            else:
                self.queues = queues

            # No need for this greenlet if we don't have any subqueues
            if not has_subqueues:
                return

            time.sleep(self.config["subqueues_refresh_interval"])

    def greenlet_paused_queues(self):

      while True:

          # Update the process-local list of paused queues
          Queue.paused_queues = Queue.redis_paused_queues()
          time.sleep(self.config["paused_queues_refresh_interval"])

    def get_memory(self):
        mmaps = self.process.get_memory_maps()
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
        for greenlet in self.gevent_pool:
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
            cpu_times = self.process.get_cpu_times()
            cpu = {
                "user": cpu_times.user,
                "system": cpu_times.system,
                "percent": self.process.get_cpu_percent(0)
            }
            mem = self.get_memory()

        # Avoid sharing passwords or sensitive config!
        whitelisted_config = [
            "max_jobs",
            "greenlets",
            "processes",
            "queues",
            "scheduler",
            "name",
            "local_ip"
        ]

        io = None
        if self._traced_io:
            io = {}
            for k, v in iteritems(self._traced_io):
                if k == "total":
                    io[k] = v
                else:
                    io[k] = sorted(list(v.items()), reverse=True, key=lambda x: x[1])

        used_pool_slots = self.pool_size - self.gevent_pool.free_count()

        return {
            "status": self.status,
            "config": {k: v for k, v in iteritems(self.config) if k in whitelisted_config},
            "done_jobs": self.done_jobs,
            "pool_usage_average": self.pool_usage_average.next(used_pool_slots),
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

        from gevent import wsgi

        def admin_routes(env, start_response):
            path = env["PATH_INFO"]
            status = "200 OK"
            res = ""
            if path in ["/", "/report", "/report_mem"]:
                report = self.get_worker_report(with_memory=(path == "/report_mem"))
                res = bytes(json_stdlib.dumps(report, cls=MongoJSONEncoder), 'utf-8')
            elif path == "/wait_for_idle":
                self.idle_wait_count = 0
                self.idle_event.clear()
                self.idle_event.wait()
                res = "idle"
            else:
                status = "404 Not Found"
            start_response(status, [('Content-Type', 'application/json')])
            return [res]

        server = wsgi.WSGIServer((self.config["admin_ip"], self.config["admin_port"]), admin_routes, log=Devnull())

        try:
            self.log.debug("Starting admin server on port %s" % self.config["admin_port"])
            server.serve_forever()
        except Exception as e:  # pylint: disable=broad-except
            self.log.debug("Error in admin server : %s" % e)

    def flush_logs(self, w=0):
        self.log_handler.flush(w=w)

    def work(self):
        """Starts the work loop.

        """
        self.work_init()

        self.work_loop(max_jobs=self.max_jobs)

        return self.work_stop()

    def work_init(self):

        self.connect()

        self.status = "started"

        # An interval of 0 disables the refresh
        if self.config["subqueues_refresh_interval"] > 0:
            self.greenlets["subqueues"] = gevent.spawn(self.greenlet_subqueues)

        # An interval of 0 disables the refresh
        if self.config["paused_queues_refresh_interval"] > 0:
            self.greenlets["paused_queues"] = gevent.spawn(self.greenlet_paused_queues)

        if self.config["report_interval"] > 0:
            self.greenlets["report"] = gevent.spawn(self.greenlet_report)
            self.greenlets["logs"] = gevent.spawn(self.greenlet_logs)

        if self.config["scheduler"] and self.config["scheduler_interval"] > 0:
            self.greenlets["scheduler"] = gevent.spawn(self.greenlet_scheduler)

        if self.config["admin_port"]:
            self.greenlets["admin"] = gevent.spawn(self.greenlet_admin)

        self.install_signal_handlers()

    def work_loop(self, max_jobs=None):

        self.done_jobs = 0
        self.idle_wait_count = 0

        # has_raw = any(q.is_raw or q.is_sorted for q in [Queue(x) for x in self.queues])

        try:

            queue_offset = 0

            while True:

                if self.graceful_stop:
                    break

                # If the scheduler greenlet are crashed, fail loudly.
                if self.config["scheduler"] and not self.greenlets["scheduler"]:
                    self.exitcode = 6
                    break

                while True:

                    free_pool_slots = self.gevent_pool.free_count()

                    if max_jobs:
                        total_started = (self.pool_size - free_pool_slots) + self.done_jobs
                        free_pool_slots = min(free_pool_slots, max_jobs - total_started)
                        if free_pool_slots == 0:
                            self.exitcode = 5
                            break

                    if free_pool_slots > 0:
                        self.status = "wait"
                        break
                    self.status = "full"
                    gevent.sleep(0.01)

                jobs = []

                available_queues = [
                    queue for queue in self.queues
                    if queue.root_id not in Queue.paused_queues and
                    queue.id not in Queue.paused_queues
                ]

                for queue_i in range(len(available_queues)):

                    queue = available_queues[(queue_i + queue_offset) % len(available_queues)]

                    max_jobs_per_queue = free_pool_slots - len(jobs)

                    if max_jobs_per_queue <= 0:
                        queue_i -= 1
                        break

                    if self.config["dequeue_strategy"] == "parallel":
                        max_jobs_per_queue = max(1, int(max_jobs_per_queue / (len(available_queues) - queue_i)))

                    jobs += queue.dequeue_jobs(
                        max_jobs=max_jobs_per_queue,
                        job_class=self.job_class,
                        worker=self
                    )

                # At the next pass, start at the next queue to avoid always dequeuing the same one
                if self.config["dequeue_strategy"] == "parallel":
                    queue_offset = (queue_offset + queue_i + 1) % len(self.queues)

                for job in jobs:

                    # TODO investigate spawn_raw?
                    self.gevent_pool.spawn(self.perform_job, job)

                # TODO consider this when dequeuing jobs to have strict limits
                if max_jobs and self.done_jobs >= max_jobs:
                    self.log.info("Reached max_jobs=%s" % self.done_jobs)
                    break

                # We seem to have exhausted available jobs, we can sleep for a
                # while.
                if len(jobs) == 0:

                    if self.config["dequeue_strategy"] == "burst":
                        self.log.info("Burst mode: stopping now because queues were empty")
                        break

                    if (
                        not self.idle_event.is_set() and
                        len(jobs) == 0 and
                        free_pool_slots == self.pool_size and
                        self.idle_wait_count > 0
                    ):
                        self.idle_event.set()

                    self.status = "wait"
                    self.idle_wait_count += 1
                    gevent.sleep(min(self.config["max_latency"], 0.001 * self.idle_wait_count))

                # We got some jobs, reset the idle counter.
                else:
                    self.idle_wait_count = 0

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
        self.flush_logs(w=1)

        g_time = getattr(self.greenlet, "_trace_time", 0)
        g_switches = getattr(self.greenlet, "_trace_switches", None)
        self.log.debug(
            "Exiting main worker greenlet (%0.5fs, %s switches)." %
            (g_time, g_switches))

        return self.exitcode

    def perform_job(self, job):
        """ Wraps a job.perform() call with timeout logic and exception handlers.

            This is the first call happening inside the greenlet.
        """

        if self.config["trace_memory"]:
            job.trace_memory_start()

        set_current_job(job)

        gevent_timeout = None
        if job.timeout:

            gevent_timeout = gevent.Timeout(
                job.timeout,
                TimeoutInterrupt(
                    'Job exceeded maximum timeout value in greenlet (%d seconds).' %
                    job.timeout
                )
            )

            gevent_timeout.start()

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

            if gevent_timeout:
                gevent_timeout.cancel()

            set_current_job(None)

            self.done_jobs += 1

            if self.config["trace_memory"]:
                job.trace_memory_stop()

    def shutdown_graceful(self):
        """ Graceful shutdown: waits for all the jobs to finish. """

        # This is in the 'exitcodes' list in supervisord so processes
        # exiting gracefully won't be restarted.
        self.exitcode = 2

        self.log.info("Graceful shutdown...")
        raise StopRequested()  # pylint: disable=nonstandard-exception

    def shutdown_max_memory(self):

        # Not in the exitcodes list: we want it to be restarted.
        self.exitcode = 4

        self.log.info("Max memory reached, shutdown...")
        self.graceful_stop = True

    def shutdown_now(self):
        """ Forced shutdown: interrupts all the jobs. """

        # This is in the 'exitcodes' list in supervisord so processes
        # exiting gracefully won't be restarted.
        self.exitcode = 3

        self.log.info("Forced shutdown...")
        self.status = "killing"

        self.gevent_pool.kill(exception=JobInterrupt, block=False)

        raise StopRequested()  # pylint: disable=nonstandard-exception

    def install_signal_handlers(self):
        """ Handle events like Ctrl-C from the command line. """

        self.graceful_stop = False

        def request_shutdown_now():
            self.shutdown_now()

        def request_shutdown_graceful():

            # Second time CTRL-C, shutdown now
            if self.graceful_stop:
                request_shutdown_now()
            else:
                self.graceful_stop = True
                self.shutdown_graceful()

        # First time CTRL-C, try to shutdown gracefully
        gevent.signal(signal.SIGINT, request_shutdown_graceful)

        # User (or Heroku) requests a stop now, just mark tasks as interrupted.
        gevent.signal(signal.SIGTERM, request_shutdown_now)
