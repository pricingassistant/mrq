from mrq.task import Task
import time


class Square(Task):
    """ Returns the square of an integer """
    def run(self, params):
        return int(params["n"]) ** 2


class CPU(Task):
    """ A CPU-intensive task """
    def run(self, params):
        for n in range(int(params["n"])):
            n ** n
        return params["a"]


class IO(Task):
    """ An IO-intensive task """
    def run(self, params):
        time.sleep(float(params["sleep"]))
        return params["a"]
