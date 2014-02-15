#!/usr/bin/env python
import os
import argparse
import tempfile

os.chdir(os.path.dirname(__file__))

from . import add_standard_worker_arguments


def parse_args():

  parser = argparse.ArgumentParser(description='Starts an RQ worker managed by supervisord')

  parser.add_argument('--command', action='store', default=os.environ.get("SUPERVISORD_COMMAND", "python worker.py highpriority default lowpriority"), help='Command to start the worker')
  parser.add_argument('--template', action='store', default=os.environ.get("SUPERVISORD_TEMPLATE", "docker"), help='Name of supervisord template to use')
  parser.add_argument('--processes', default=int(os.environ.get("SUPERVISORD_PROCESSES", 1)), type=int, action='store', help='Number of processes to start')

  add_standard_worker_arguments(parser)

  return parser.parse_args()

args = parse_args()

# We wouldn't need to do all that if supervisord supported environment variables in all its config fields!
with open("supervisord/%s.conf" % args.template, "r") as f:
  conf = f.read()

fh, path = tempfile.mkstemp(prefix="supervisordconfig")
f = os.fdopen(fh, "w")
f.write(conf.replace("{{{SUPERVISORD_COMMAND}}}", args.command).replace("{{{SUPERVISORD_PROCESSES}}}", str(args.processes)))
f.close()

try:
  print "Starting: supervisord -c %s" % path
  os.system("supervisord -c %s" % path)
finally:
  os.remove(path)
