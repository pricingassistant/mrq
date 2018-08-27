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
from ..utils import normalize_command
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

        # Evaluate what workers are currently, rightfully there. They won't be touched.
        for agent in agents:
            desired_workers = self.get_desired_workers_for_agent(group, agent)
            agent["new_desired_workers"] = []
            agent["new_desired_workers"] = desired_workers

        for agent in agents:
            if sorted(agent["new_desired_workers"]) != sorted(agent.get("desired_workers", [])):
                connections.mongodb_jobs.mrq_agents.update_one({"_id": agent["_id"]}, {"$set": {
                    "desired_workers": agent["new_desired_workers"]
                }})

        # Remember the date of the last successful orchestration (will be reported)
        self.dateorchestrated = datetime.datetime.utcnow()

        log.debug("Orchestration finished.")

    def redis_queuestats_key(self):
        """ Returns the global HSET redis key used to store queue stats """
        return "%s:queuestats" % (get_current_config()["redis_prefix"])

    def get_desired_workers_for_agent(self, group, agent):
        return group.get("commands", [])

    def fetch_worker_group_reports(self, worker_group, projection=None):
        return list(connections.mongodb_jobs.mrq_workers.find({
            "config.worker_group": worker_group["_id"]
        }, projection=projection))

    def fetch_worker_group_definitions(self):

        definitions = list(connections.mongodb_jobs.mrq_workergroups.find())

        for definition in definitions:
            commands = []
            # Prepend all commands by their worker group.
            for command in definition.get("commands", []):
                simplified_command, worker_count = normalize_command(command, definition["_id"])
                commands.extend([simplified_command] * worker_count)
            definition["commands"] = commands

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
