from .context import get_current_config, connections, log, run_task, metric
import time
import datetime
import gevent
import argparse
import random
import shlex
import traceback
from collections import defaultdict
from bson import ObjectId
from redis.lock import LuaLock
from .processes import Process, ProcessPool
from .utils import MovingETA, normalize_command
from .queue import Queue


class Agent(Process):
    """ MRQ Agent manages its local worker pool and takes turns in orchestrating the others in its group. """

    def __init__(self, worker_group=None):
        self.greenlets = {}
        self.id = ObjectId()
        self.worker_group = worker_group or get_current_config()["worker_group"]
        self.pool = ProcessPool(extra_env={
            "MRQ_AGENT_ID": str(self.id),
            "MRQ_WORKER_GROUP": self.worker_group
        })
        self.config = get_current_config()
        self.status = "started"
        metric("agent", data={"worker_group": self.worker_group, "agent_id": self.id})
        self.dateorchestrated = None

        # global redis key used to ensure only one agent orchestrator runs at a time
        self.redis_queuestats_lock_key = "%s:queuestatslock" % (self.config["redis_prefix"])

        # global HSET redis key used to store queue stats
        self.redis_queuestats_key= "%s:queuestats" % (self.config["redis_prefix"])

    def work(self):

        self.install_signal_handlers()
        self.datestarted = datetime.datetime.utcnow()

        self.pool.start()
        self.manage()

        self.greenlets["orchestrate"] = gevent.spawn(self.greenlet_orchestrate)
        self.greenlets["orchestrate"].start()

        self.greenlets["manage"] = gevent.spawn(self.greenlet_manage)
        self.greenlets["manage"].start()

        # Disabled for now
        # self.greenlets["queuestats"] = gevent.spawn(self.greenlet_queuestats)
        # self.greenlets["queuestats"].start()

        try:
            self.pool.wait()
        finally:
            self.shutdown_now()
            self.status = "stop"
            self.manage()

    def shutdown_now(self):
        self.pool.terminate()

        for g in self.greenlets.values():
            g.kill()

    def shutdown_graceful(self):
        self.pool.stop(timeout=None)

    def greenlet_manage(self):
        """ This greenlet always runs in background to update current status
            in MongoDB every N seconds.
         """

        while True:
            try:
                self.manage()
            except Exception as e:  # pylint: disable=broad-except
                log.error("When reporting: %s" % e)
            finally:
                time.sleep(self.config["report_interval"])

    def manage(self):

        report = self.get_agent_report()

        try:
            db = connections.mongodb_jobs.mrq_agents.find_and_modify({
                "_id": ObjectId(self.id)
            }, {"$set": report}, upsert=True)
            if not db:
                return
        except Exception as e:  # pylint: disable=broad-except
            log.debug("Agent report failed: %s" % e)
            return

        # If the desired_workers was changed by an orchestrator, apply the changes locally
        if self.status != "stop" and sorted(db.get("desired_workers", [])) != sorted(self.pool.desired_commands):

            group = self.fetch_worker_group_definition()
            process_termination_timeout = float(group.get("process_termination_timeout") or 60)
            self.pool.set_commands(db.get("desired_workers", []), timeout=process_termination_timeout)

    def get_agent_report(self):
        report = {
            "current_workers": [p["command"] for p in self.pool.processes],
            "total_cpu": get_current_config()["total_cpu"],
            "total_memory": get_current_config()["total_memory"],
            "worker_group": self.worker_group,
            "status": self.status,
            "dateorchestrated": self.dateorchestrated,
            "datestarted": self.datestarted,
            "datereported": datetime.datetime.utcnow(),
            "dateexpires": datetime.datetime.utcnow() + datetime.timedelta(seconds=(self.config["report_interval"] * 3) + 5)
        }
        metric("agent", data={"worker_group": self.worker_group, "agent_id": self.id, "worker_count": len(self.pool.processes)})
        return report

    def greenlet_orchestrate(self):
        while True:
            try:
                self.orchestrate()
            except Exception as e:
                log.error("Orchestration error! %s" % e)
                traceback.print_exc()

            time.sleep(self.config["orchestrate_interval"])

    def orchestrate(self):
        run_task("mrq.basetasks.orchestrator.Orchestrate", {})

    def greenlet_queuestats(self):

        interval = min(self.config["orchestrate_interval"], 1 * 60)
        lock_timeout = 5 * 60 + (interval * 2)

        while True:
            lock = LuaLock(connections.redis, self.redis_queuestats_lock_key,
                           timeout=lock_timeout, thread_local=False, blocking=False)
            with lock:
                lock_expires = time.time() + lock_timeout
                self.queue_etas = defaultdict(lambda: MovingETA(5))

                while True:
                    self.queuestats()

                    # Because queue stats can be expensive, we try to keep the lock on the same agent
                    lock_extend = (time.time() + lock_timeout) - lock_expires
                    lock_expires += lock_extend
                    lock.extend(lock_extend)

                    time.sleep(interval)

            time.sleep(interval)

    def queuestats(self):
        """ Compute ETAs for every known queue & subqueue """

        start_time = time.time()
        log.debug("Starting queue stats...")

        # Fetch all known queues
        queues = [Queue(q) for q in Queue.all_known()]

        new_queues = {queue.id for queue in queues}
        old_queues = set(self.queue_etas.keys())

        for deleted_queue in old_queues.difference(new_queues):
            self.queue_etas.pop(deleted_queue)

        t = time.time()
        stats = {}

        for queue in queues:
            cnt = queue.count_jobs_to_dequeue()
            eta = self.queue_etas[queue.id].next(cnt, t=t)

            # Number of jobs to dequeue, ETA, Time of stats
            stats[queue.id] = "%d %s %d" % (cnt, eta if eta is not None else "N", int(t))

        with connections.redis.pipeline(transaction=True) as pipe:
            if random.randint(0, 100) == 0 or len(stats) == 0:
                pipe.delete(self.redis_queuestats_key)
            if len(stats) > 0:
                pipe.hmset(self.redis_queuestats_key, stats)
            pipe.execute()

        log.debug("... done queue stats in %0.4fs" % (time.time() - start_time))

    def fetch_worker_group_definition(self):
        definition = connections.mongodb_jobs.mrq_workergroups.find_one({"_id": self.worker_group})

        # Prepend all commands by their worker profile.
        commands = []
        for command in definition.get("commands", []):
            simplified_command, worker_count = normalize_command(command, self.worker_group)
            commands.extend([simplified_command] * worker_count)

        definition["commands"] = commands
        return definition
