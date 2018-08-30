#!/usr/bin/env python
from __future__ import print_function

import os
import sys
is_pypy = '__pypy__' in sys.builtin_module_names

# Needed to make getaddrinfo() work in pymongo on Mac OS X
# Docs mention it's a better choice for Linux as well.
# This must be done asap in the worker

if "GEVENT_RESOLVER" not in os.environ and not is_pypy:
    os.environ["GEVENT_RESOLVER"] = "ares"

# We must still monkey-patch the methods for job sub-pools.
from gevent import monkey
monkey.patch_all()

import argparse
import ujson as json
import json as json_stdlib
import datetime

sys.path.insert(0, os.getcwd())

from mrq import config, utils
from mrq.context import set_current_config, set_logger_config, set_current_job, connections
from mrq.job import queue_job
from mrq.utils import load_class_by_path, MongoJSONEncoder

def main():

    parser = argparse.ArgumentParser(description='Runs a task')

    cfg = config.get_config(parser=parser, config_type="run", sources=("file", "env", "args"))
    cfg["is_cli"] = True
    set_current_config(cfg)
    set_logger_config()

    if len(cfg["taskargs"]) == 1:
        params = json.loads(cfg["taskargs"][0])  # pylint: disable=no-member
    else:
        params = {}

        # mrq-run taskpath a 1 b 2 => {"a": "1", "b": "2"}
        for group in utils.group_iter(cfg["taskargs"], n=2):
            if len(group) != 2:
                print("Number of arguments wasn't even")
                sys.exit(1)
            params[group[0]] = group[1]

    if cfg["queue"]:
        ret = queue_job(cfg["taskpath"], params, queue=cfg["queue"])
        print(ret)
    else:
        worker_class = load_class_by_path(cfg["worker_class"])
        job = worker_class.job_class(None)
        job.set_data({
            "path": cfg["taskpath"],
            "params": params,
            "queue": cfg["queue"]
        })
        job.datestarted = datetime.datetime.utcnow()
        set_current_job(job)
        ret = job.perform()
        print(json_stdlib.dumps(ret, cls=MongoJSONEncoder))  # pylint: disable=no-member

    # This shouldn't be needed as the process will exit and close any remaining sockets
    # connections.redis.connection_pool.disconnect()

if __name__ == "__main__":
    main()
