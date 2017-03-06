from future.builtins import object


class Task(object):

    # Are we the first task that a Job called?
    is_main_task = False
    max_concurrency = 0

    # Default write concern values when setting status=success
    # http://docs.mongodb.org/manual/reference/write-concern/
    status_success_update_w = None
    status_success_update_j = None

    def __init__(self):
        pass

    def run_wrapped(self, params):
        """ Override this method to provide your own wrapping code """
        return self.run(params)

    def run(self, params):
        """ Override this method with the main code of all your tasks """
        raise NotImplementedError
