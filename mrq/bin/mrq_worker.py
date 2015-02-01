#!/usr/bin/env python
import os

# Needed to make getaddrinfo() work in pymongo on Mac OS X
# Docs mention it's a better choice for Linux as well.
# This must be done asap in the worker
if "GEVENT_RESOLVER" not in os.environ:
    os.environ["GEVENT_RESOLVER"] = "ares"

from gevent import monkey
monkey.patch_all()

import sys
import tempfile
import signal
import subprocess32 as subprocess
import psutil
import argparse

sys.path.insert(0, os.getcwd())

from mrq import config
from mrq.utils import load_class_by_path
from mrq.context import set_current_config


def main():

    parser = argparse.ArgumentParser(description='Start a RQ worker')

    cfg = config.get_config(parser=parser, config_type="worker", sources=("file", "env", "args"))

    # If we are launching with a --processes option and without the SUPERVISOR_ENABLED env
    # then we should just call supervisord.
    if cfg["processes"] > 0 and not os.environ.get("SUPERVISOR_ENABLED"):

        # We wouldn't need to do all that if supervisord supported environment
        # variables in all its config fields!
        with open(cfg["supervisord_template"], "r") as f:
            conf = f.read()

        fh, path = tempfile.mkstemp(prefix="mrqsupervisordconfig")
        f = os.fdopen(fh, "w")

        # We basically relaunch ourselves, but the config will contain the
        # MRQ_SUPERVISORD_ISWORKER env.
        conf = conf.replace("{{ SUPERVISORD_COMMAND }}", " ".join(sys.argv))
        conf = conf.replace(
            "{{ SUPERVISORD_PROCESSES }}", str(cfg["processes"]))

        f.write(conf)
        f.close()

        try:

            # start_new_session=True avoids sending the current process'
            # signals to the child.
            process = subprocess.Popen(
                ["supervisord", "-c", path], start_new_session=True)

            def sigint_handler(signum, frame):  # pylint: disable=unused-argument

                # At this point we need to send SIGINT to all workers. Unfortunately supervisord
                # doesn't support this, so we have to find all the children pids and send them the
                # signal ourselves :-/
                # https://github.com/Supervisor/supervisor/issues/179
                #
                psutil_process = psutil.Process(process.pid)
                worker_processes = psutil_process.get_children(recursive=False)

                if len(worker_processes) == 0:
                    return process.send_signal(signal.SIGTERM)

                for child_process in worker_processes:
                    child_process.send_signal(signal.SIGINT)

                # Second time sigint is used, we should terminate supervisord itself which
                # will send SIGTERM to all the processes anyway.
                signal.signal(signal.SIGINT, sigterm_handler)

                # Wait for all the childs to finish
                for child_process in worker_processes:
                    child_process.wait()

                # Then stop supervisord itself.
                process.send_signal(signal.SIGTERM)

            def sigterm_handler(signum, frame):  # pylint: disable=unused-argument
                process.send_signal(signal.SIGTERM)

            signal.signal(signal.SIGINT, sigint_handler)
            signal.signal(signal.SIGTERM, sigterm_handler)

            process.wait()

        finally:
            os.remove(path)

    # If not, start the actual worker
    else:

        worker_class = load_class_by_path(cfg["worker_class"])

        set_current_config(cfg)

        w = worker_class()

        exitcode = w.work_loop()

        sys.exit(exitcode)

if __name__ == "__main__":
    main()
