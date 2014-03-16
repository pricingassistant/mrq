import pytest
import os
try:
  import subprocess32 as subprocess
except:
  import subprocess
import sys
import psutil
import time
import re
import json
import urllib2

sys.path.append(os.getcwd())

from mrq.job import Job
from mrq.queue import send_tasks
from mrq.config import get_config
from mrq.utils import wait_for_net_service
from mrq.context import connections, set_current_config

set_current_config(get_config(sources=("env")))

os.system("rm -rf dump.rdb")


class ProcessFixture(object):
  def __init__(self, request, cmdline=None, wait_port=None, quiet=False):
    self.request = request
    self.cmdline = cmdline
    self.process = None
    self.wait_port = wait_port
    self.quiet = quiet
    self.stopped = False

    self.request.addfinalizer(self.stop)

  def start(self, cmdline=None, env=None, expected_children=0):

    self.stopped = False
    self.process_children = []

    if not cmdline:
      cmdline = self.cmdline
    if env is None:
      env = {}

    # Kept from parent env
    for env_key in ["PATH", "GEVENT_LOOP", "VIRTUAL_ENV"]:
      if os.environ.get(env_key) and not env.get(env_key):
        env[env_key] = os.environ.get(env_key)

    if self.quiet:
      stdout = open(os.devnull, 'w')
    else:
      stdout = None

    self.cmdline = cmdline
    # print cmdline
    self.process = subprocess.Popen(re.split(r"\s+", cmdline) if type(cmdline) in [str, unicode] else cmdline,
                                    shell=False, close_fds=True, env=env, cwd=os.getcwd(), stdout=stdout)

    if self.quiet:
      stdout.close()

    # Wait for children to start
    if expected_children > 0:
      psutil_process = psutil.Process(self.process.pid)

      # print "Expecting %s children, got %s" % (expected_children, psutil_process.get_children(recursive=False))
      while True:
        self.process_children = psutil_process.get_children(recursive=True)
        if len(self.process_children) >= expected_children:
          break
        time.sleep(0.1)

    if self.wait_port:
      wait_for_net_service("127.0.0.1", int(self.wait_port))

  def stop(self, force=False, timeout=None, block=True, sig=15):

    # Call this only one time.
    if self.stopped and not force:
      return
    self.stopped = True

    if self.process is not None:

      os.kill(self.process.pid, sig)

      # When sending a sigkill to the process, we also want to kill the children in case of supervisord usage
      if sig == 9 and len(self.process_children) > 0:
        for c in self.process_children:
          c.send_signal(sig)

      if not block:
        return

      for _ in range(500):

        try:
          p = psutil.Process(self.process.pid)
          if p.status == "zombie":
            # print "process %s zombie OK" % self.cmdline
            return
        except psutil.NoSuchProcess:
          # print "process %s exit OK" % self.cmdline
          return

        time.sleep(0.01)

      assert False, "Process '%s' was still in state %s after 5 seconds..." % (self.cmdline, p.status)


class WorkerFixture(ProcessFixture):

  def __init__(self, request, **kwargs):
    ProcessFixture.__init__(self, request, cmdline=kwargs.get("cmdline"))

    self.fixture_mongodb = kwargs["mongodb"]
    self.fixture_redis = kwargs["redis"]

    self.started = False

  def start(self, flush=True, deps=True, trace=True, **kwargs):

    self.started = True

    if deps:
      self.start_deps(flush=flush)

    processes = 0
    m = re.search(r"--processes (\d+)", kwargs.get("flags", ""))
    if m:
      processes = int(m.group(1))

    cmdline = "python mrq/bin/mrq_worker.py --trace_mongodb --mongodb_logs_size 0 %s %s %s %s" % (
      "--admin_port 20020" if (processes <= 1) else "",
      "--trace_greenlets" if trace else "",
      kwargs.get("flags", ""),
      kwargs.get("queues", "high default low")
    )

    # +1 because of supervisord itself
    if processes > 0:
      processes += 1
    ProcessFixture.start(self, cmdline=cmdline, env=kwargs.get("env"), expected_children=processes)

  def start_deps(self, flush=True):

    self.fixture_mongodb.start()
    self.fixture_redis.start()

    # Will auto-connect
    connections.reset()

    self.mongodb_jobs = connections.mongodb_jobs
    self.mongodb_logs = connections.mongodb_logs
    self.redis = connections.redis

    if flush:
      self.fixture_mongodb.flush()
      self.fixture_redis.flush()

  def stop(self, deps=True, sig=2, **kwargs):

    if self.started:
      ProcessFixture.stop(self, sig=sig, **kwargs)

    if deps:
      self.stop_deps(**kwargs)

  def stop_deps(self, **kwargs):
     self.fixture_mongodb.stop(sig=2, **kwargs)
     self.fixture_redis.stop(sig=2, **kwargs)

  def send_tasks(self, path, params_list, block=True, queue=None, accept_statuses=["success"]):
    if not self.started:
      self.start()

    job_ids = send_tasks(path, params_list, queue=queue)

    if not block:
      return job_ids

    results = []

    for job_id in job_ids:
      job = Job(job_id).wait(poll_interval=0.01)
      assert job.get("status") in accept_statuses

      results.append(job.get("result"))

    return results

  def send_task(self, path, params, **kwargs):
    return self.send_tasks(path, [params], **kwargs)[0]

  def send_task_cli(self, path, params, block=True, queue=None, **kwargs):
    if not self.started and not block:
      self.start()

    cli = ["python", "mrq/bin/mrq_run.py", "--quiet"]
    if queue:
      cli += ["--queue", queue]
    if not block:
      cli += ["--async"]
    cli += [path, json.dumps(params)]

    out = subprocess.check_output(cli).strip()
    if block:
      return json.loads(out)
    return out

  def get_report(self):
    wait_for_net_service("127.0.0.1", 20020)
    f = urllib2.urlopen("http://127.0.0.1:20020")
    data = json.load(f)
    f.close()
    return data


class RedisFixture(ProcessFixture):
  def flush(self):
    connections.redis.flushdb()


class MongoFixture(ProcessFixture):
  def flush(self):
    for mongodb in (connections.mongodb_jobs, connections.mongodb_logs):
      if mongodb:
        for c in mongodb.collection_names():
          if not c.startswith("system."):
            mongodb.drop_collection(c)


@pytest.fixture(scope="function")
def httpstatic(request):
  return ProcessFixture(request, "/usr/sbin/nginx -c /app/tests/fixtures/httpstatic/nginx.conf", wait_port=8081)


@pytest.fixture(scope="function")
def mongodb(request):
  cmd = "mongod --smallfiles --noprealloc --nojournal"
  if os.environ.get("STACK_STARTED"):
    cmd = "sleep 1h"
  return MongoFixture(request, cmd, wait_port=27017, quiet=True)


@pytest.fixture(scope="function")
def redis(request):
  cmd = "redis-server"
  if os.environ.get("STACK_STARTED"):
    cmd = "sleep 1h"
  return RedisFixture(request, cmd, wait_port=6379, quiet=True)


@pytest.fixture(scope="function")
def worker(request, mongodb, redis):

  return WorkerFixture(request, mongodb=mongodb, redis=redis)

