import argparse
import os
import sys
from .version import VERSION
from .utils import get_local_ip
import atexit


def add_parser_args(parser, config_type):

  # General arguments

  parser.add_argument('--trace_greenlets', action='store_true', default=False,
                      help='Collect stats about each greenlet execution time and switches.')

  parser.add_argument('--trace_memory', action='store_true', default=False,
                      help='Collect stats about memory for each task. Incompatible with gevent > 1')

  parser.add_argument('--trace_mongodb', action='store_true', default=False,
                      help='Collect stats about MongoDB requests')

  parser.add_argument('--print_mongodb', action='store_true', default=False,
                      help='Print all MongoDB requests')

  parser.add_argument('--trace_memory_type', action='store', default="",
                      help='Create a .png object graph in trace_memory_output_dir with a random object of this type.')

  parser.add_argument('--trace_memory_output_dir', action='store', default="memory_traces",
                      help='Directory where to output .pngs with object graphs')

  parser.add_argument('--profile', action='store_true', default=False,
                      help='Run profiling on the whole worker')

  parser.add_argument('--mongodb_jobs', action='store', default="mongodb://127.0.0.1:27017/mrq",
                      help='MongoDB URI for the jobs, scheduled_jobs & workers database')

  parser.add_argument('--mongodb_logs', action='store', default="mongodb://127.0.0.1:27017/mrq",
                      help='MongoDB URI for the logs database. If set to "0", will disable remote logs.')

  parser.add_argument('--mongodb_logs_size', action='store', default=16 * 1024 * 1024, type=int,
                      help='If provided, sets the log collection to capped to that amount of bytes.')

  parser.add_argument('--redis', action='store', default="redis://127.0.0.1:6379",
                      help='Redis URI')

  parser.add_argument('--redis_prefix', action='store', default="mrq",
                      help='Redis key prefix')

  parser.add_argument('--name', default=None, action='store',
                      help='Specify a different name')

  parser.add_argument('--quiet', default=False, action='store_true',
                      help='Don\'t output task logs')

  parser.add_argument('--config', '-c', default=None, action="store",
                      help='Path of a config file')

  parser.add_argument('--worker_class', default="mrq.worker.Worker", action="store",
                      help='Path to a custom worker class')

  parser.add_argument('--version', '-v', default=False, action="store_true",
                      help='Prints current MRQ version')

  parser.add_argument('--no_import_patch', default=False, action='store_true',
                      help='Skips patching __import__ to fix gevent bug #108')

  # mrq-run-specific arguments

  if config_type == "run":

    parser.add_argument('--async', action='store_true', default=False,
                        help='Queue the task instead of running it right away')

    parser.add_argument('--queue', action='store', default="default",
                        help='Queue where to put the task when async')

    parser.add_argument('taskpath', action='store',
                        help='Task to run')

    parser.add_argument('taskargs', action='store', default='{}', nargs='*',
                        help='JSON-encoded arguments, or "key value" pairs')

  # Dashboard-specific arguments

  elif config_type == "dashboard":

    parser.add_argument('--dashboard_httpauth', default="", action="store",
                        help='HTTP Auth for the Dashboard. Format is user:pass')

    parser.add_argument('--dashboard_queue', default=None, action="store",
                        help='Default queue for dashboard actions.')

  # Worker-specific args

  elif config_type == "worker":

    parser.add_argument('--max_jobs', default=0, type=int, action='store',
                        help='Gevent: max number of jobs to do before quitting. Temp workaround for memory leaks')

    parser.add_argument('--gevent', '-g', default=1, type=int, action='store',
                        help='Gevent: max number of greenlets')

    parser.add_argument('--processes', '-p', default=0, type=int, action='store',
                        help='Number of processes to launch with supervisord')

    default_template = os.path.abspath(os.path.join(os.path.dirname(__file__), "supervisord_templates/default.conf"))
    parser.add_argument('--supervisord_template', default=default_template, action='store',
                        help='Path of supervisord template to use')

    parser.add_argument('--scheduler', default=False, action='store_true',
                        help='Run the scheduler')

    parser.add_argument('--scheduler_interval', default=60, action='store', type=int,
                        help='Seconds between scheduler checks')

    parser.add_argument('--report_interval', default=10, action='store', type=int,
                        help='Seconds between worker reports to MongoDB')

    parser.add_argument('queues', nargs='*', default=["default"],
                        help='The queues to listen on (default: \'default\')')

    parser.add_argument('--admin_port', default=0, action="store", type=int,
                        help='Start an admin server on this port, if provided. Incompatible with --processes')

    parser.add_argument('--local_ip', default=get_local_ip(), action="store", type=str,
                        help='Overwrite the local IP, to be displayed in the dashboard.')


def get_config(sources=("file", "env", "args"), env_prefix="MRQ_", parser=None, config_type="worker"):

  if not parser:
    parser = argparse.ArgumentParser()

  add_parser_args(parser, config_type)

  if config_type in ["run"]:
    default_config = parser.parse_args(["x"]).__dict__
  else:
    default_config = parser.parse_args([]).__dict__

  # Keys that can't be passed from the command line
  default_config["tasks"] = {}
  default_config["scheduled_tasks"] = {}

  # Only keep values different from config, actually passed on the command line
  from_args = {}
  if "args" in sources:
    for k, v in parser.parse_args().__dict__.iteritems():
      if default_config[k] != v:
        from_args[k] = v

  # If we were given another config file, use it
  if from_args.get("config"):
    config_file = from_args.get("config")
  # If a mrq-config.py file is in the current directory, use it!
  elif os.path.isfile(os.path.join(os.getcwd(), "mrq-config.py")):
    config_file = os.path.join(os.getcwd(), "mrq-config.py")
  else:
    config_file = None

  from_file = {}
  if config_file and "file" in sources:
    sys.path.insert(0, os.path.dirname(config_file))
    config_module = __import__(os.path.basename(config_file.replace(".py", "")))
    sys.path.pop(0)
    for k, v in config_module.__dict__.iteritems():

      # We only keep variables starting with an uppercase character.
      if k[0].isupper():
        default_config[k.lower()] = v
        if k.lower() not in default_config:
          default_config[k.lower()] = v

  # Merge the config in the order given by the user
  merged_config = default_config
  for part in sources:
    for name in merged_config:

      if part == "env":
        value = os.environ.get(env_prefix + name.upper())
        if value:
          merged_config[name] = value
      elif part == "args" and name in from_args:
        merged_config[name] = from_args[name]
      elif part == "file" and name in from_file:
        merged_config[name] = from_file[name]

  if merged_config["profile"]:
    import cProfile
    profiler = cProfile.Profile()
    profiler.enable()

    def print_profiling():
      profiler.print_stats(sort="cumulative")

    atexit.register(print_profiling)

  if merged_config["version"]:
    print "MRQ version: %s" % VERSION
    print "Python version: %s" % sys.version
    sys.exit(1)

  return merged_config
