from future.builtins import object

from .context import get_current_config, connections, log
import time
import json
import gevent
from bson import ObjectId
from collections import defaultdict
from .processes import Process, ProcessPool


class Agent(Process):
    """ MRQ Agent manages its local worker pool and takes turns in orchestrating the others in its group. """

    def __init__(self, worker_group=None):
        self.greenlets = {}
        self.id = ObjectId()
        self.worker_group = worker_group or get_current_config()["worker_group"]
        self.pool = ProcessPool()
        self.config = get_current_config()

    def work(self):

        self.install_signal_handlers()

        self.greenlets["orchestrate"] = gevent.spawn(self.greenlet_orchestrate)
        self.greenlets["orchestrate"].start()

        self.greenlets["manage"] = gevent.spawn(self.greenlet_manage)
        self.greenlets["manage"].start()

        self.pool.start()

        self.pool.wait()

    def shutdown_now(self):
        self.pool.terminate()

        self.greenlets["orchestrate"].kill()
        self.greenlets["manage"].kill()

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
        if sorted(db.get("desired_workers", [])) != sorted(self.pool.desired_commands):
            self.pool.set_commands(db.get("desired_workers", []))

    def get_agent_report(self):
        report = {
            "current_workers": [p["command"] for p in self.pool.processes],
            "available_cpu": get_current_config()["available_cpu"],
            "available_memory": get_current_config()["available_memory"],
            "worker_group": self.worker_group
        }
        return report

    def greenlet_orchestrate(self):

        while True:
            with connections.redis.lock(self.redis_agent_orchestrator_key, timeout=self.config["orchestrate_interval"] + 10):
                self.orchestrate()
                time.sleep(self.config["orchestrate_interval"])

    @property
    def redis_agent_orchestrator_key(self):
        """ Returns the global redis key used to ensure only one agent orchestrator runs at a time """
        return "%s:agentorchestrator:%s" % (get_current_config()["redis_prefix"], self.worker_group)

    def orchestrate(self):
        """ Executed periodically on one of the agents, to manage the desired workers of *all* the agents in its group """

        group = self.fetch_worker_group_definition()
        if not group:
            log.error("Worker group %s has no definition yet. Can't orchestrate!" % self.worker_group)
            return

        agents = self.fetch_worker_group_agents()

        desired_workers = self.get_desired_workers_for_group(group)

        # Evaluate what workers are currently, rightfully there. They won't be touched.
        current_workers = defaultdict(int)
        for agent in agents:
            agent["free_memory"] = agent["available_memory"]
            agent["free_cpu"] = agent["available_cpu"]
            agent["new_desired_workers"] = []
            for worker in agent.get("desired_workers", []):
                if worker in desired_workers:
                    cpu = desired_workers[worker]["cpu"]
                    memory = desired_workers[worker]["memory"]

                    # If no more memory for currently existing workers: their requirements must have changed.
                    # We need to schedule it somewhere else
                    if cpu <= agent["free_cpu"] and memory <= agent["free_memory"]:
                        current_workers[worker] += 1
                        agent["free_cpu"] -= cpu
                        agent["free_memory"] -= memory
                        agent["new_desired_workers"].append(worker)

        # What changes need to be made in worker count
        deltas = {
            worker: (worker_info["desired_count"] - current_workers[worker])
            for worker, worker_info in desired_workers.items()
            if worker_info["desired_count"] != current_workers[worker]
        }

        # Remove workers from the most loaded machines (TODO improve)
        for worker, delta in deltas.items():
            if delta >= 0:
                continue

            for _ in range(delta, 0):
                found = False
                for agent in sorted(agents, key=lambda a: float(a["free_cpu"]) / a["available_cpu"]):
                    for i in range(len(agent["new_desired_workers"])):
                        if agent["new_desired_workers"][i] == worker:
                            agent["new_desired_workers"].pop(i)
                            agent["free_cpu"] += desired_workers[worker]["cpu"]
                            agent["free_memory"] += desired_workers[worker]["memory"]
                            found = True
                            break
                    if found:
                        break

                assert found

        # Add new workers to the least loaded machines
        for worker, delta in deltas.items():
            if delta <= 0:
                continue

            cpu = desired_workers[worker]["cpu"]
            memory = desired_workers[worker]["memory"]

            for _ in range(delta):
                found = False
                for agent in sorted(agents, key=lambda a: float(a["free_cpu"]) / a["available_cpu"], reverse=True):
                    if cpu <= agent["free_cpu"] and memory <= agent["free_memory"]:
                        agent["new_desired_workers"].append(worker)
                        agent["free_cpu"] -= cpu
                        agent["free_memory"] -= memory
                        found = True
                        break

                if not found:
                    log.debug("Worker orchestration: no agent had enough CPU & memory (%s & %s) to schedule a new worker" % (cpu, memory))
                    # TODO: communicate the need for new resources
                    break

        for agent in agents:
            if sorted(agent["new_desired_workers"]) != sorted(agent.get("desired_workers", [])):
                # Commit the changes in DB
                connections.mongodb_jobs.mrq_agents.update_one({"_id": agent["_id"]}, {"$set": {
                    "desired_workers": agent["new_desired_workers"],
                    "free_cpu": agent["free_cpu"],
                    "free_memory": agent["free_memory"]
                }})

    def get_desired_workers_for_group(self, group):

        workers = {}

        for profile in group.get("profiles", []):
            workers[profile["command"]] = {
                "desired_count": profile["min_count"],  # TODO!
                "memory": profile["memory"],
                "cpu": profile["cpu"]
            }

        return workers

    def fetch_worker_group_agents(self):
        return list(connections.mongodb_jobs.mrq_agents.find({"worker_group": self.worker_group}))

    def fetch_worker_group_definition(self):
        return connections.mongodb_jobs.mrq_workergroups.find_one({"_id": self.worker_group})

