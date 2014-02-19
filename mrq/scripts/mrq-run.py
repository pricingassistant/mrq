#!/usr/bin/env python
# from gevent import monkey
# monkey.patch_all()

import sys
import os
import argparse
import json

sys.path.append(os.getcwd())

from mrq import worker, config, queue, utils


parser = argparse.ArgumentParser(description='Runs a task')

parser.add_argument('--async', action='store_true', default=False, help='Queue the task instead of running it right away')
parser.add_argument('--queue', action='store', default="default", help='Queue where to put the task when async')
parser.add_argument('taskpath', action='store', help='Task to run')
parser.add_argument('taskargs', action='store', default='{}', nargs='*', help='JSON-encoded arguments, or "key value" pairs')

args = parser.parse_args()

if len(args.taskargs) == 1:
  params = json.loads(args.taskargs)
else:
  params = {}

  # mrq-run taskpath a 1 b 2 => {"a": "1", "b": "2"}
  for group in utils.group_iter(args.taskargs, n=2):
    if len(group) != 2:
      print "Number of arguments wasn't even"
      sys.exit(1)
    params[group[0]] = group[1]


def main():

  worker.Worker(config.get_config())

  queue.send_task(args.taskpath, params, sync=not args.async, queue=args.queue)

if __name__ == "__main__":
  main()
