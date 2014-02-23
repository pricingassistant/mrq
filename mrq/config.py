import argparse
import os
import sys


def get_config(sources=("file", "env", "args"), env_prefix="MRQ_", defaults=None):

  parser = argparse.ArgumentParser(description='Starts an RQ worker')

  parser.add_argument('--max_jobs', default=0, type=int, action='store',
                      help='Gevent: max number of jobs to do before quitting. Temp workaround for memory leaks')

  parser.add_argument('--pool_size', '-n', default=1, type=int, action='store',
                      help='Gevent: max number of greenlets')

  parser.add_argument('--mongodebug', action='store_true', default=False,
                      help='Print all Mongo requests')

  parser.add_argument('--objgraph', action='store_true', default=False,
                      help='Start objgraph to debug memory after each task')

  parser.add_argument('--profile', action='store_true', default=False,
                      help='Run profiling on the whole worker')

  parser.add_argument('--mongodb_jobs', action='store', default="mongodb://127.0.0.1:27017/mrq",
                      help='MongoDB URI for the jobs database')

  parser.add_argument('--mongodb_logs', action='store', default="mongodb://127.0.0.1:27017/mrq",
                      help='MongoDB URI for the logs database')

  parser.add_argument('--redis', action='store', default="redis://127.0.0.1:6379",
                      help='Redis URI')

  parser.add_argument('--redis_prefix', action='store', default="mrq",
                      help='Redis key prefix')

  parser.add_argument('--name', default=None, action='store',
                      help='Specify a different name')

  parser.add_argument('--quiet', default=False, action='store_true',
                      help='Don\'t output task logs')

  parser.add_argument('--scheduler', default=False, action='store_true',
                      help='Run the scheduler')

  parser.add_argument('--scheduler_interval', default=60, action='store', type=int,
                      help='Seconds between scheduler checks')

  parser.add_argument('--report_interval', default=10, action='store', type=int,
                      help='Seconds between worker reports to MongoDB')

  parser.add_argument('--config', '-c', default=None, action="store",
                      help='Path of a config file')

  parser.add_argument('queues', nargs='*', default=["default"],
                      help='The queues to listen on (default: \'default\')')

  if "args" in sources:
    from_args = parser.parse_args()
  else:
    from_args = parser.parse_args([])

  # Get defaults
  merged_config = from_args.__dict__
  if defaults is not None:
    merged_config.update(defaults)

  config_module = None
  if "file" in sources and merged_config["config"]:
    sys.path.append(os.path.dirname(merged_config["config"]))
    config_module = __import__(os.path.basename(merged_config["config"].replace(".py", "")))
    sys.path.pop(-1)
    merged_config.update({k.lower(): v for k, v in config_module.__dict__.iteritems()})

  # Keys that can't be passed from the command line
  merged_config["tasks"] = {}
  merged_config["scheduled_tasks"] = {}

  for part in sources:
    for name, arg_value in merged_config.iteritems():

      value = None
      if part == "env":
        value = os.environ.get(env_prefix + name.upper())
      elif part == "args":
        value = arg_value
      elif part == "file":
        value = getattr(config_module, name.upper(), None)

      if value is not None:
        merged_config[name] = value

  return merged_config
