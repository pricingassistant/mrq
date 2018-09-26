from __future__ import print_function
from future.builtins import str
import argparse
import os
import sys
import re
import psutil
from .version import VERSION
from .utils import get_local_ip
import atexit
import logging


def add_parser_args(parser, config_type):

    # General arguments

    parser.add_argument(
        '--trace_greenlets',
        action='store_true',
        default=False,
        help='Collect stats about each greenlet execution time and switches.')

    parser.add_argument(
        '--trace_memory',
        action='store_true',
        default=False,
        help='Collect stats about memory for each task. Incompatible with --greenlets > 1')

    parser.add_argument(
        '--trace_io',
        action='store_true',
        default=False,
        help='Collect stats about all I/O operations')

    parser.add_argument(
        '--print_mongodb',
        action='store_true',
        default=False,
        help='Print all MongoDB requests')

    parser.add_argument(
        '--trace_memory_type',
        action='store',
        default="",
        help='Create a .png object graph in trace_memory_output_dir ' +
             'with a random object of this type.')

    parser.add_argument(
        '--trace_memory_output_dir',
        action='store',
        default="memory_traces",
        help='Directory where to output .pngs with object graphs')

    parser.add_argument(
        '--profile',
        action='store_true',
        default=False,
        help='Run profiling on the whole worker')

    parser.add_argument(
        '--mongodb_jobs', '--mongodb',
        action='store',
        default="mongodb://127.0.0.1:27017/mrq",
        help='MongoDB URI for the jobs, scheduled_jobs & workers database')

    parser.add_argument(
        '--mongodb_logs',
        action='store',
        default="1",
        help='MongoDB URI for the logs database. ' +
             ' "0" will disable remote logs, "1" will use main MongoDB.')

    parser.add_argument(
        '--redis',
        action='store',
        default="redis://127.0.0.1:6379",
        help='Redis URI')

    parser.add_argument(
        '--redis_prefix',
        action='store',
        default="mrq",
        help='Redis key prefix')

    parser.add_argument(
        '--redis_max_connections',
        action='store',
        type=int,
        default=1000,
        help='Redis max connection pool size')

    parser.add_argument(
        '--redis_timeout',
        action='store',
        type=float,
        default=30,
        help='Redis connection pool timeout to wait for an available connection')

    parser.add_argument(
        '--name',
        default=None,
        action='store',
        help='Specify a different name')

    parser.add_argument(
        '--config',
        '-c',
        default=None,
        action="store",
        help='Path of a config file')

    parser.add_argument(
        '--worker_class',
        default="mrq.worker.Worker",
        action="store",
        help='Path to a custom worker class')

    parser.add_argument(
        '--quiet',
        default=False,
        action='store_true',
        help='Don\'t output task logs')

    parser.add_argument(
        '--queue_ttl',
        default=24 * 60 * 60,
        action='store',
        help='Seconds the Queue size is stored in redis'
    )

    parser.add_argument(
        '--log_handler',
        default="mrq.logger.MongoHandler",
        action="store",
        help='Path to a log handler class')

    parser.add_argument(
        '--log_format',
        default="%(asctime)s [%(levelname)s] %(message)s",
        action="store",
        help='log format')

    parser.add_argument(
        '--log_level',
        default="DEBUG",
        action="store",
        help='set the logging level')

    parser.add_argument(
        '--version',
        '-v',
        default=False,
        action="store_true",
        help='Prints current MRQ version')

    parser.add_argument(
        '--no_import_patch',
        default=False,
        action='store_true',
        help='(DEPRECATED) Skips patching __import__ to fix gevent bug #108')

    parser.add_argument(
        '--add_network_latency',
        default="0",
        action='store',
        type=str,
        help='Adds random latency to the network calls, zero to N seconds. Can be a range (1-2)')

    parser.add_argument(
        '--default_job_ttl',
        default=180 * 24 * 3600,
        action='store',
        type=float,
        help='Seconds the tasks are kept in MongoDB when statuses are not success, abort, cancel and started')

    parser.add_argument(
        '--default_job_result_ttl',
        default=7 * 24 * 3600,
        action='store',
        type=float,
        help='Seconds the results are kept in MongoDB when status is success')

    parser.add_argument(
        '--default_job_abort_ttl',
        default=24 * 3600,
        action='store',
        type=float,
        help='Seconds the tasks are kept in MongoDB when status is abort')

    parser.add_argument(
        '--default_job_cancel_ttl',
        default=24 * 3600,
        action='store',
        type=float,
        help='Seconds the tasks are kept in MongoDB when status is cancel')

    parser.add_argument(
        '--default_job_timeout',
        default=3600,
        action='store',
        type=float,
        help='In seconds, delay before interrupting the job')

    parser.add_argument(
        '--default_job_max_retries',
        default=3,
        action='store',
        type=int,
        help='Set the status to "maxretries" after retrying that many times')

    parser.add_argument(
        '--default_job_retry_delay',
        default=3,
        action='store',
        type=int,
        help='Seconds before a job in retry status is requeued again')

    parser.add_argument(
        '--use_large_job_ids',
        action='store_true',
        default=False,
        help='Do not use compacted job IDs in Redis. For compatibility with 0.1.x only')

    # mrq-run-specific arguments

    if config_type == "run":

        parser.add_argument(
            '--queue',
            action='store',
            default="",
            help='Queue the task on this queue instead of running it right away')

        parser.add_argument(
            'taskpath',
            action='store',
            help='Task to run')

        parser.add_argument(
            'taskargs',
            action='store',
            default='{}',
            nargs='*',
            help='JSON-encoded arguments, or "key value" pairs')

    # Dashboard-specific arguments

    elif config_type == "dashboard":

        parser.add_argument(
            '--dashboard_httpauth',
            default="",
            action="store",
            help='HTTP Auth for the Dashboard. Format is user:pass')

        parser.add_argument(
            '--dashboard_queue',
            default=None,
            action="store",
            help='Default queue for dashboard actions.')

        parser.add_argument(
            '--dashboard_port',
            default=5555,
            action="store",
            type=int,
            help='Use this port for mrq-dashboard. 5555 by default.')

        parser.add_argument(
            '--dashboard_ip',
            default="0.0.0.0",
            action="store",
            type=str,
            help='Bind the dashboard to this IP. Default is "0.0.0.0", use "127.0.0.1" to restrict access.')

    # Agent-specific args
    elif config_type == "agent":

        parser.add_argument(
            '--worker_group',
            default="default",
            action="store",
            type=str,
            help='The name of the worker group to manage')

        parser.add_argument(
            '--total_memory',
            default=int(psutil.virtual_memory().total * 0.8 / (1024 * 1024)),
            action="store",
            type=int,
            help="How much memory MB this agent's workers can use. Used for scheduling, not a hard limit.")

        parser.add_argument(
            '--total_cpu',
            default=psutil.cpu_count(logical=True) * 1024,
            action="store",
            type=int,
            help="How much CPU units this agent's workers can use. We recommend using 1024 per CPU.")

        parser.add_argument(
            '--orchestrate_interval',
            default=30,
            action="store",
            type=float,
            help="How much seconds to wait between orchestration runs.")

        parser.add_argument(
            '--report_interval',
            default=10,
            action='store',
            type=float,
            help='Seconds between agent reports to MongoDB')

        parser.add_argument(
            '--autoscaling_taskpath',
            default=None,
            action='store',
            type=str,
            help='Path to a task that can perform autoscaling actions during orchestration')

    # Worker-specific args
    elif config_type == "worker":

        parser.add_argument(
            '--max_jobs',
            default=0,
            type=int,
            action='store',
            help='Gevent: max number of jobs to do before quitting.' +
                 ' Temp workaround for memory leaks')

        parser.add_argument(
            '--max_time',
            default=0.0,
            type=float,
            action='store',
            help='Max time a worker should run before quitting.')

        parser.add_argument(
            '--max_memory',
            default=0,
            type=int,
            action='store',
            help='Max memory (in Mb) after which the process will be shut down. Use with mrq-agent' +
                 'to automatically respawn the worker when this happens')

        parser.add_argument(
            '--greenlets',
            '--gevent',  # deprecated
            '-g',
            default=1,
            type=int,
            action='store',
            help='Max number of greenlets to use')

        parser.add_argument(
            '--processes',
            '-p',
            default=0,
            type=int,
            action='store',
            help='Number of processes to launch')

        parser.add_argument(
            '--scheduler',
            default=False,
            action='store_true',
            help='Run the scheduler')

        parser.add_argument(
            '--ensure_indexes',
            default=False,
            action='store_true',
            help='Ensures the internal MongoDB indexes of MRQ are built, or does so in the background')

        parser.add_argument(
            '--scheduler_interval',
            default=60,
            action='store',
            type=float,
            help='Seconds between scheduler checks')

        parser.add_argument(
            '--report_interval',
            default=10,
            action='store',
            type=float,
            help='Seconds between worker reports to MongoDB')

        parser.add_argument(
            '--report_file',
            default="",
            action='store',
            type=str,
            help='Filepath of a json dump of the worker status. Disabled if none')

        parser.add_argument(
            '--agent_id',
            default="",
            action='store',
            type=str,
            help='ID of the Agent this worker process is linked to')

        parser.add_argument(
            '--worker_id',
            default="",
            action='store',
            type=str,
            help='ID of this worker process. Should be left empty to be autogenerated in most cases.')

        parser.add_argument(
            '--worker_group',
            default="",
            action='store',
            type=str,
            help='Worker group of the agent this worker was launched from')

        parser.add_argument(
            '--task_whitelist',
            default="",
            action='store',
            type=str,
            help='Comma-separated list of task paths to do exclusively on this worker. Only for regular queues.')

        parser.add_argument(
            '--task_blacklist',
            default="",
            action='store',
            type=str,
            help='Comma-separated list of task paths to do exclude from this worker. Only for regular queues.')

        parser.add_argument(
            'queues',
            nargs='*',
            default=["default"],
            help='The queues to listen on (default: \'default\')')

        parser.add_argument(
            '--subqueues_refresh_interval',
            default=60,
            action='store',
            type=float,
            help="Seconds between worker refreshes of the known subqueues")

        parser.add_argument(
            '--paused_queues_refresh_interval',
            default=10,
            action='store',
            type=float,
            help="Seconds between worker refreshes of the paused queues list")

        parser.add_argument(
            '--admin_port',
            default=0,
            action="store",
            type=int,
            help='Start an admin server on this port, if provided. Incompatible with --processes')

        parser.add_argument(
            '--admin_ip',
            default="127.0.0.1",
            action="store",
            type=str,
            help='IP for the admin server to listen on. Use "0.0.0.0" to allow access from outside')

        parser.add_argument(
            '--local_ip',
            default=get_local_ip(),
            action="store",
            type=str,
            help='Overwrite the local IP, to be displayed in the dashboard.')

        parser.add_argument(
            '--external_ip',
            default=None,
            action="store",
            type=str,
            help='Overwrite the external IP, to be displayed in the dashboard.')

        parser.add_argument(
            '--max_latency',
            default=1.,
            type=float,
            action='store',
            help='Max seconds while worker may sleep waiting for a new job. ' +
                 'Can be < 1.')

        parser.add_argument(
            '--dequeue_strategy',
            default="sequential",
            type=str,
            action='store',
            help='Strategy for dequeuing multiple queues. Default is \'sequential\',' +
                 'to dequeue them in command-line order.')


class ArgumentParserIgnoringDefaults(argparse.ArgumentParser):
    def add_argument(self, *args, **kwargs):
        kwargs.pop("default", None)
        return argparse.ArgumentParser.add_argument(self, *args, **kwargs)


def get_config(
        sources=(
            "file",
            "env"),
        env_prefix="MRQ_",
        file_path=None,
        parser=None,
        extra=None,
        config_type=None):
    """ Returns a config dict merged from several possible sources """

    if not parser:
        parser = argparse.ArgumentParser()

    add_parser_args(parser, config_type)
    parser_types = {action.dest: action.type for action in parser._actions if action.dest}

    if config_type in ["run"]:
        default_config = parser.parse_args(["notask"]).__dict__
    else:
        default_config = parser.parse_args([]).__dict__

    # Keys that can't be passed from the command line
    default_config["tasks"] = {}
    default_config["log_handlers"] = {}
    default_config["scheduled_tasks"] = {}

    # Only keep values actually passed on the command line
    from_args = {}
    if "args" in sources:
        cmdline_parser = ArgumentParserIgnoringDefaults(argument_default=argparse.SUPPRESS)
        add_parser_args(cmdline_parser, config_type)
        from_args = cmdline_parser.parse_args().__dict__

    # If we were given another config file, use it

    if file_path is not None:
        config_file = file_path
    elif from_args.get("config"):
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
        for k, v in config_module.__dict__.items():

            # We only keep variables starting with an uppercase character.
            if k[0].isupper():
                from_file[k.lower()] = v

    # Merge the config in the order given by the user
    merged_config = default_config

    config_keys = set(list(default_config.keys()) + list(from_file.keys()))

    for part in sources:
        for name in config_keys:

            if part == "env":
                value = os.environ.get(env_prefix + name.upper())
                if value:
                    if name == "queues":
                        value = re.split("\s+", value)
                    if parser_types.get(name):
                        value = parser_types[name](value)
                    merged_config[name] = value
            elif part == "args" and name in from_args:
                merged_config[name] = from_args[name]
            elif part == "file" and name in from_file:
                merged_config[name] = from_file[name]

    if extra:
        merged_config.update(extra)

    if merged_config["profile"]:
        import cProfile, pstats
        profiler = cProfile.Profile()
        profiler.enable()

        def print_profiling():
            pstats.Stats(profiler).sort_stats("cumulative").print_stats()

        atexit.register(print_profiling)

    if merged_config["version"]:
        print("MRQ version: %s" % VERSION)
        print("Python version: %s" % sys.version)
        sys.exit(1)

    if "no_import_patch" in from_args:
        print("WARNING: --no_import_patch will be deprecated in MRQ 1.0!")

    return merged_config
