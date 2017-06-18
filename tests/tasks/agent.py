from mrq.task import Task
from mrq.context import connections


class Autoscale(Task):

    def run(self, params):

        connections.mongodb_jobs.tests_inserts.insert({"params": params}, manipulate=False)

        return params
