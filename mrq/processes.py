from future.builtins import object
import psutil
import os
import time
import signal
import shlex
import gevent

try:
    import subprocess32 as subprocess
except:
    import subprocess

from .context import log


class Process(object):
    """ The parent class of Worker, Agent and Supervisor """

    exitcode = 0

    def install_signal_handlers(self):
        """ Handle events like Ctrl-C from the command line. """

        self.graceful_stop = False

        def request_shutdown_now():
            self.shutdown_now()

        def request_shutdown_graceful():

            # Second time CTRL-C, shutdown now
            if self.graceful_stop:
                self.shutdown_now()
            else:
                self.graceful_stop = True
                self.shutdown_graceful()

        # First time CTRL-C, try to shutdown gracefully
        gevent.signal(signal.SIGINT, request_shutdown_graceful)

        # User (or Heroku) requests a stop now, just mark tasks as interrupted.
        gevent.signal(signal.SIGTERM, request_shutdown_now)


class ProcessPool(object):
    """ Manages a pool of processes """

    def __init__(self, watch_interval=1, extra_env=None):
        self.processes = []
        self.desired_commands = []
        self.greenlet_watch = None
        self.watch_interval = watch_interval
        self.stopping = False
        self.extra_env = extra_env

    def set_commands(self, commands, timeout=None):
        """ Sets the processes' desired commands for this pool and manages diff to reach that state """
        self.desired_commands = commands

        target_commands = list(self.desired_commands)
        for process in list(self.processes):
            found = False
            for i in range(len(target_commands)):
                if process["command"] == target_commands[i]:
                    target_commands.pop(i)
                    found = True
                    break

            if not found:
                self.stop_process(process, timeout)

        # What is left are the commands to add
        # TODO: we should only do this once memory conditions allow
        for command in target_commands:
            self.spawn(command)

    def spawn(self, command):
        """ Spawns a new process and adds it to the pool """

        # process_name
        # output
        # time before starting (wait for port?)
        # start_new_session=True : avoid sending parent signals to child

        env = dict(os.environ)
        env["MRQ_IS_SUBPROCESS"] = "1"
        env.update(self.extra_env or {})

        # Extract env variables from shell commands.
        parts = shlex.split(command)
        for p in list(parts):
            if "=" in p:
                env[p.split("=")[0]] = p[len(p.split("=")[0]) + 1:]
                parts.pop(0)
            else:
                break

        p = subprocess.Popen(parts, shell=False, close_fds=True, env=env, cwd=os.getcwd())

        self.processes.append({
            "subprocess": p,
            "pid": p.pid,
            "command": command,
            "psutil": psutil.Process(pid=p.pid)
        })

    def start(self):
        self.greenlet_watch = gevent.spawn(self.watch)
        self.greenlet_watch.start()

    def wait(self):
        """ Waits for the pool to be fully stopped """

        while True:
            if not self.greenlet_watch:
                break

            if self.stopping:
                gevent.sleep(0.1)
            else:
                gevent.sleep(1)

    def watch(self):

        while True:
            self.watch_processes()
            gevent.sleep(self.watch_interval)

    def watch_processes(self):
        """ Manages the status of all the known processes """

        for process in list(self.processes):
            self.watch_process(process)

        # Cleanup processes
        self.processes = [p for p in self.processes if not p.get("dead")]

        if self.stopping and len(self.processes) == 0:
            self.stop_watch()

    def watch_process(self, process):
        """ Manages the status of a single process """

        status = process["psutil"].status()

        # TODO: how to avoid zombies?
        # print process["pid"], status

        if process.get("terminate"):
            if status in ("zombie", "dead"):
                process["dead"] = True
            elif process.get("terminate_at"):
                if time.time() > (process["terminate_at"] + 5):
                    log.warning("Process %s had to be sent SIGKILL" % (process["pid"], ))
                    process["subprocess"].send_signal(signal.SIGKILL)
                elif time.time() > process["terminate_at"]:
                    log.warning("Process %s had to be sent SIGTERM" % (process["pid"], ))
                    process["subprocess"].send_signal(signal.SIGTERM)

        else:
            if status in ("zombie", "dead"):
                # Restart a new process right away (TODO: sleep a bit? max retries?)
                process["dead"] = True
                self.spawn(process["command"])

            elif status not in ("running", "sleeping"):
                log.warning("Process %s was in status %s" % (process["pid"], status))

                # process["subprocess"].returncode in (0, 2, 3)

    def stop(self, timeout=None):
        """ Initiates a graceful stop of the processes """

        self.stopping = True

        for process in list(self.processes):
            self.stop_process(process, timeout=timeout)

    def stop_process(self, process, timeout=None):
        """ Initiates a graceful stop of one process """

        process["terminate"] = True
        if timeout is not None:
            process["terminate_at"] = time.time() + timeout
        process["subprocess"].send_signal(signal.SIGINT)

    def terminate(self):
        """ Terminates the processes right now with a SIGTERM """

        for process in list(self.processes):
            process["subprocess"].send_signal(signal.SIGTERM)

        self.stop_watch()

    def kill(self):
        """ Kills the processes right now with a SIGKILL """

        for process in list(self.processes):
            process["subprocess"].send_signal(signal.SIGKILL)

        self.stop_watch()

    def stop_watch(self):
        """ Stops the periodic watch greenlet, thus the pool itself """

        if self.greenlet_watch:
            self.greenlet_watch.kill(block=False)
            self.greenlet_watch = None
