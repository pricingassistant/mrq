from mrq.task import Task
from mrq.context import connections


class EnsureIndexes(Task):

    def run(self, params):

        if connections.mongodb_logs:
            connections.mongodb_logs.mrq_logs.ensure_index(
                [("job", 1)], background=True)
            connections.mongodb_logs.mrq_logs.ensure_index(
                [("worker", 1)], background=True, sparse=True)

        connections.mongodb_jobs.mrq_workers.ensure_index(
            [("status", 1)], background=True)
        connections.mongodb_jobs.mrq_workers.ensure_index(
            [("datereported", 1)], background=True, expireAfterSeconds=3600)

        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("status", 1)], background=True)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("path", 1)], background=True)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("worker", 1)], background=True, sparse=True)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("queue", 1)], background=True)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("dateexpires", 1)], sparse=True, background=True, expireAfterSeconds=0)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("dateretry", 1)], sparse=True, background=True)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("datequeued", 1)], background=True)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("queue", 1), ("status", 1), ("datequeued", 1), ("_id", 1)], background=True)
        connections.mongodb_jobs.mrq_jobs.ensure_index(
            [("status", 1), ("queue", 1), ("path", 1)], background=True)

        connections.mongodb_jobs.mrq_scheduled_jobs.ensure_index(
            [("hash", 1)], unique=True, background=False)

        connections.mongodb_jobs.mrq_agents.ensure_index(
            [("datereported", 1)], background=True)
        connections.mongodb_jobs.mrq_agents.ensure_index(
            [("dateexpires", 1)], background=True, expireAfterSeconds=0)
        connections.mongodb_jobs.mrq_agents.ensure_index(
            [("worker_group", 1)], background=True)
