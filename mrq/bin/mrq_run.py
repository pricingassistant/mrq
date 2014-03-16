#!/usr/bin/env python

# We must still monkey-patch the methods for job sub-pools.
from gevent import monkey
monkey.patch_all()

import sys
import os
import argparse
import ujson as json
import datetime

sys.path.insert(0, os.getcwd())

from mrq import config, queue, utils
from mrq.context import set_current_config, set_current_job
from mrq.utils import load_class_by_path

parser = argparse.ArgumentParser(description='Runs a task')


parser.add_argument('--quiet', action='store_true', default=False, help='No logging')
parser.add_argument('--async', action='store_true', default=False, help='Queue the task instead of running it right away')
parser.add_argument('--queue', action='store', default="default", help='Queue where to put the task when async')
parser.add_argument('taskpath', action='store', help='Task to run')
parser.add_argument('taskargs', action='store', default='{}', nargs='*', help='JSON-encoded arguments, or "key value" pairs')

args = parser.parse_args()

if len(args.taskargs) == 1:
  params = json.loads(args.taskargs[0])
else:
  params = {}

  # mrq-run taskpath a 1 b 2 => {"a": "1", "b": "2"}
  for group in utils.group_iter(args.taskargs, n=2):
    if len(group) != 2:
      print "Number of arguments wasn't even"
      sys.exit(1)
    params[group[0]] = group[1]


def main():

  cfg = config.get_config(sources=("file", "env"))
  cfg["quiet"] = args.quiet
  cfg["is_cli"] = True
  set_current_config(cfg)

  if args.async:
    ret = queue.send_task(args.taskpath, params, sync=False, queue=args.queue)
    print ret
  else:
    worker_class = load_class_by_path(cfg["worker_class"])
    job = worker_class.job_class(None)
    job.data = {
      "path": args.taskpath,
      "params": params,
      "queue": args.queue
    }
    job.datestarted = datetime.datetime.utcnow()
    set_current_job(job)
    ret = job.perform()
    print json.dumps(ret)

if __name__ == "__main__":
  main()
