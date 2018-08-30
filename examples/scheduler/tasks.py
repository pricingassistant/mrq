
from mrq.task import Task


class Print(Task):

    def run(self, params):

        print("Hello world !")
        print(params["x"])
