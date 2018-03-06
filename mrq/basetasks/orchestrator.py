from future.builtins import str
from mrq.queue import Queue
from mrq.task import Task
from mrq.job import Job
from mrq.context import log, connections, run_task, get_current_config, subpool_map
from collections import defaultdict
import math
import shlex
import argparse
from ..config import add_parser_args
import traceback
import datetime
import re


class Orchestrate(Task):

    max_concurrency = 1

    def run(self, params):

        self.config = get_current_config()

        concurrency = int(params.get("concurrency", 5))
        groups = self.fetch_worker_group_definitions()
        if len(groups) == 0:
            log.error("No worker group definition yet. Can't orchestrate!")
            return

        subpool_map(concurrency, self.orchestrate, groups)

    def redis_orchestrator_lock_key(self, worker_group):
        """ Returns the global redis key used to ensure only one agent orchestrator runs at a time """
        return "%s:orchestratorlock:%s" % (get_current_config()["redis_prefix"], worker_group)

    def orchestrate(self, worker_group):
        try:
            self.do_orchestrate(worker_group)
        except Exception as e:
            log.error("Orchestration error! %s" % e)
            traceback.print_exc()

    def do_orchestrate(self, group):
        """ Manage the desired workers of *all* the agents in the given group """

        log.debug("Starting orchestration run for worker group %s" % group["_id"])

        agents = self.fetch_worker_group_agents(group)

        desired_workers = self.get_desired_workers_for_group(group, agents)
        print "here desired", len(desired_workers), "/", len(agents)

        # Evaluate what workers are currently, rightfully there. They won't be touched.
        current_workers = defaultdict(int)
        for agent in agents:
            agent["free_memory"] = agent["total_memory"]
            agent["free_cpu"] = agent["total_cpu"]
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
                for agent in sorted(agents, key=lambda a: float(a["free_cpu"]) / a["total_cpu"]):
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

        needs_new_agents = False

        # Add new workers to the least loaded machines
        for worker, delta in deltas.items():
            if delta <= 0:
                continue

            cpu = desired_workers[worker]["cpu"]
            memory = desired_workers[worker]["memory"]

            for _ in range(delta):
                found = False
                for agent in sorted(agents, key=lambda a: float(a["free_cpu"]) / a["total_cpu"], reverse=True):
                    if cpu <= agent["free_cpu"] and memory <= agent["free_memory"]:
                        agent["new_desired_workers"].append(worker)
                        agent["free_cpu"] -= cpu
                        agent["free_memory"] -= memory
                        found = True
                        break

                if not found:
                    log.debug("Worker orchestration: no agent had enough CPU & memory (%s & %s) to schedule a new worker" % (cpu, memory))
                    needs_new_agents = True
                    break

        # Worker diversity enforcement: if we are under-scaled, make sure that at least one worker
        # of each profile is launched. if not, make space forcefully on other workers.
        if needs_new_agents:

            all_profiles = defaultdict(int)
            for agent in agents:
                for w in agent["new_desired_workers"]:
                    all_profiles[w] += 1

            removable = sorted([list(x) for x in all_profiles.items() if x[1] > 1], key=lambda item: item[1], reverse=True)

            for profile in desired_workers:
                if desired_workers[profile]["desired_count"] == 0:
                    continue
                if profile not in all_profiles:
                    found = False
                    # This profile is not currently represented in the workers. Make some space for it.
                    for rem in removable:
                        for agent in agents:
                            for i in range(len(agent["new_desired_workers"])):
                                if agent["new_desired_workers"][i] == rem[0] and rem[1] > 1:
                                    agent["new_desired_workers"][i] = None
                                    agent["free_cpu"] += desired_workers[rem[0]]["cpu"]
                                    agent["free_memory"] += desired_workers[rem[0]]["memory"]
                                    rem[1] -= 1
                                if desired_workers[profile]["cpu"] <= agent["free_cpu"] and desired_workers[profile]["memory"] <= agent["free_memory"]:
                                    log.debug("Orchestration: enforcing worker diversity with %s => %s" % (rem[0], profile))
                                    agent["new_desired_workers"].append(profile)
                                    agent["free_cpu"] -= desired_workers[profile]["cpu"]
                                    agent["free_memory"] -= desired_workers[profile]["memory"]
                                    found = True
                                    break

                            agent["new_desired_workers"] = [x for x in agent["new_desired_workers"] if x is not None]
                            if found:
                                break

                        if found:
                            break

                    if not found:
                        log.debug("Orchestration couldn't enforce worker diversity for profile '%s'" % profile)

        # User-provided autoscaling task
        if self.config.get("autoscaling_taskpath"):
            result = run_task(self.config["autoscaling_taskpath"], {
                "agents": agents,
                "needs_new_agents": needs_new_agents,
                "worker_group": group["_id"]
            })
            needs_new_agents = result["needs_new_agents"]
            agents = result["agents"]

        # Save the new values in the DB. They will be applied by each agent process.
        connections.mongodb_jobs.worker_groups.update_one({"_id": group["_id"]}, {"$set": {
            "needs_new_agents": needs_new_agents
        }})

        for agent in agents:
            if sorted(agent["new_desired_workers"]) != sorted(agent.get("desired_workers", [])):
                connections.mongodb_jobs.mrq_agents.update_one({"_id": agent["_id"]}, {"$set": {
                    "desired_workers": agent["new_desired_workers"],
                    "free_cpu": agent["free_cpu"],
                    "free_memory": agent["free_memory"]
                }})

        # Remember the date of the last successful orchestration (will be reported)
        self.dateorchestrated = datetime.datetime.utcnow()

        log.debug("Orchestration finished.")

    def redis_queuestats_key(self):
        """ Returns the global HSET redis key used to store queue stats """
        return "%s:queuestats" % (get_current_config()["redis_prefix"])

    def get_desired_workers_for_group(self, group, agents):

        workers = {}

        def unpack(v):
            s = v.split(" ")
            return (int(s[0]), None if s[1] == "N" else float(s[1]), int(s[2]))

        # count_jobs, eta, last_time
        etas = {
            k: unpack(v)
            for k, v in connections.redis.hgetall(self.redis_queuestats_key).items()
        }

        # Compute average usage of each worker profile
        worker_reports = self.fetch_worker_group_reports(group, projection=[
            "_id", "config.worker_profile", "usage_avg", "status", "datestarted"  # , "process.cpu", "process.mem"
        ])

        worker_warmups_by_profile = defaultdict(int)
        worker_usages_by_profile = defaultdict(list)
        for rep in worker_reports:
            profileid = rep["config"].get("worker_profile")
            if not profileid or rep.get("status") not in ("wait", "spawn", "full"):
                continue
            # Don't take brand new workers into account yet.
            age = (datetime.datetime.utcnow() - rep["datestarted"]).total_seconds()
            if age < group.get("profiles", {}).get(profileid, {}).get("warmup", 60):
                worker_warmups_by_profile[profileid] += 1
                continue
            worker_usages_by_profile[profileid].append(rep["usage_avg"])

        worker_count_by_profile = defaultdict(int)
        for agent in agents:
            for command in agent.get("desired_workers", []):
                profile = re.search(r"^MRQ_WORKER_PROFILE=([^\s]+)", command)
                if profile:
                    profile = profile.group(1)
                    worker_count_by_profile[profile] += 1

        # Compute the desired count for each profile
        # This is the real "autoscaling" part.
        print "here group profiles", group["_id"], len(group.get("profiles") or [])
        from pprint import pprint
        pprint(worker_count_by_profile)
        for profileid, profile in group.get("profiles", {}).items():

            cfg = self.get_config_for_profile(profile)
            eta_queues = [q for q in cfg.queues if (q in etas and etas[q][1] is not None)]
            worker_usage = sum(worker_usages_by_profile.get(profileid, []))
            report_count = len(worker_usages_by_profile.get(profileid, []))
            current_desired_count = worker_count_by_profile[profileid]
            total_greenlets = (cfg.greenlets or 1) * (cfg.processes or 1)
            warmups = worker_warmups_by_profile[profileid]

            print "here details", profileid, worker_usage, report_count, current_desired_count, total_greenlets, warmups
            # By default, try not to change count
            desired_count = current_desired_count

            # If the queue is doing OK, are all instances necessary?
            if warmups == 0 and report_count > 0:
                desired_count = math.ceil(worker_usage * (float(current_desired_count) / report_count))

            if len(eta_queues) > 0:

                total_jobs = sum(etas[q][0] for q in eta_queues)
                max_eta = max(etas[q][1] for q in eta_queues)

                max_allowed_eta = profile.get("max_eta", 3600)

                # Queue is taking too long, try to add an instance.
                if max_eta > max_allowed_eta or any(etas[q][1] < 0 for q in eta_queues):

                    # Don't add a worker if the absolute number of remaining jobs is less than the number of
                    # greenlets. (They may become available right now)
                    # Also don't add a worker if the reported, warmed-up worker count is not yet the desired one.
                    if total_jobs > total_greenlets and report_count == current_desired_count:
                        desired_count = current_desired_count + 1

            final_count = min(max(desired_count, profile.get("min_count", 0)), profile.get("max_count", 100))

            if final_count != current_desired_count:
                log.debug("Autoscaling: Changing worker profile %s count from %s to %s" % (
                    profileid, current_desired_count, final_count
                ))

            workers[profile["command"]] = {
                "desired_count": final_count,
                "memory": profile["memory"],
                "cpu": profile["cpu"]
            }

        return workers

    def fetch_worker_group_reports(self, worker_group, projection=None):
        return list(connections.mongodb_jobs.mrq_workers.find({
            "config.worker_group": worker_group["_id"]
        }, projection=projection))

    def fetch_worker_group_definitions(self):

        definitions = list(connections.mongodb_jobs.mrq_workergroups.find())

        for definition in definitions:
            # Prepend all commands by their worker profile.
            for profileid, profile in (definition or {}).get("profiles", {}).items():
                profile["command"] = "MRQ_WORKER_PROFILE=%s %s" % (profileid, profile["command"])

        return definitions

    def fetch_worker_group_agents(self, worker_group):
        return list(connections.mongodb_jobs.mrq_agents.find({"worker_group": worker_group["_id"], "status": "started"}))

    def get_config_for_profile(self, profile):
        parser = argparse.ArgumentParser()
        add_parser_args(parser, "worker")
        parts = shlex.split(profile["command"])
        if "mrq-worker" in parts:
            parts = parts[parts.index("mrq-worker") + 1:]
        return parser.parse_args(parts)
