#!/usr/bin/env python
# from gevent import monkey
# monkey.patch_all()

import sys
import os
import argparse
import json

sys.path.insert(0, os.getcwd())

from mrq import config, queue, utils
from mrq.context import set_current_config


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

  ret = queue.send_task(args.taskpath, params, sync=not args.async, queue=args.queue)
  if args.async:
    print ret
  else:
    print json.dumps(ret)
if __name__ == "__main__":
  main()
