from future.builtins import object

from .context import get_current_config, connections, log
from bson import ObjectId
from collections import defaultdict
from .processes import ProcessPool, Process


class Supervisor(Process):
    """ Manages several mrq-worker single processes """

    def __init__(self, command, numprocs=1):
        self.numprocs = numprocs
        self.command = command
        self.pool = ProcessPool()

    def work(self):

        self.install_signal_handlers()

        self.pool.set_commands([self.command] * self.numprocs)

        self.pool.start()

        self.pool.wait()

    def shutdown_now(self):
        self.pool.terminate()

    def shutdown_graceful(self):
        self.pool.stop(timeout=None)
