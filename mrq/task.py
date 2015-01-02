
class Task(object):

    # Are we the first task that a Job called?
    is_main_task = False

    def __init__(self):
        pass

    def run_wrapped(self, params):
        """ Override this method to provide your own wrapping code """
        return self.run(params)

    def run(self, params):
        """ Override this method with the main code of all your tasks """
        raise NotImplementedError
