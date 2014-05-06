#!/usr/bin/env python
import os

# Needed to make getaddrinfo() work in pymongo on Mac OS X
# Docs mention it's a better choice for Linux as well.
# This must be done asap in the worker
if "GEVENT_RESOLVER" not in os.environ:
  os.environ["GEVENT_RESOLVER"] = "ares"

# We must still monkey-patch the methods for job sub-pools.
from gevent import monkey
monkey.patch_all()

import sys
import argparse
import ujson as json
import datetime

sys.path.insert(0, os.getcwd())

from mrq import config, queue, utils
from mrq.context import set_current_config, set_current_job
from mrq.utils import load_class_by_path


def main():

  parser = argparse.ArgumentParser(description='Runs a task')

  cfg = config.get_config(parser=parser, config_type="run")
  cfg["is_cli"] = True
  set_current_config(cfg)

  if len(cfg["taskargs"]) == 1:
    params = json.loads(cfg["taskargs"][0])
  else:
    params = {}

    # mrq-run taskpath a 1 b 2 => {"a": "1", "b": "2"}
    for group in utils.group_iter(cfg["taskargs"], n=2):
      if len(group) != 2:
        print "Number of arguments wasn't even"
        sys.exit(1)
      params[group[0]] = group[1]

  if cfg["async"]:
    ret = queue.send_task(cfg["taskpath"], params, sync=False, queue=cfg["queue"])
    print ret
  else:
    worker_class = load_class_by_path(cfg["worker_class"])
    job = worker_class.job_class(None)
    job.data = {
      "path": cfg["taskpath"],
      "params": params,
      "queue": cfg["queue"]
    }
    job.datestarted = datetime.datetime.utcnow()
    set_current_job(job)
    ret = job.perform()
    print json.dumps(ret)

if __name__ == "__main__":
  main()
