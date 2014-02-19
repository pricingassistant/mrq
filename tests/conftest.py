import pytest
import os
try:
  import subprocess32 as subprocess
except:
  import subprocess
import sys
import psutil
import time

sys.path.append(os.getcwd())

from mrq.worker import Worker
from mrq.queue import send_tasks, wait_for_job
from mrq.config import get_config
from mrq.utils import wait_for_net_service

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

  def start(self, cmdline=None, env=None):
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
    print cmdline
    self.process = subprocess.Popen(cmdline.split(" ") if type(cmdline) in [str, unicode] else cmdline,
                                    shell=False, close_fds=True, env=env, cwd=os.getcwd(), stdout=stdout)

    if self.quiet:
      stdout.close()

    if self.wait_port:
      wait_for_net_service("127.0.0.1", int(self.wait_port))

  def stop(self, force=False, timeout=None):

    # Call this only one time.
    if self.stopped:
      return
    self.stopped = True

    if self.process is not None:
      # print "kill -2 %s" % self.cmdline
      os.kill(self.process.pid, 2)

      for _ in range(500):

        try:
          p = psutil.Process(self.process.pid)
          if p.status == "zombie":
            return
        except psutil.NoSuchProcess:
          return

        time.sleep(0.01)

      assert False, "Process '%s' was still in state %s after 5 seconds..." % (self.cmdline, p.status)


class WorkerFixture(ProcessFixture):

  def __init__(self, request, **kwargs):
    ProcessFixture.__init__(self, request, cmdline=kwargs.get("cmdline"))

    self.fixture_mongodb = kwargs["mongodb"]
    self.fixture_redis = kwargs["redis"]

    self.started = False

  def start(self, **kwargs):

    self.started = True

    self.fixture_mongodb.start()
    self.fixture_redis.start()

    cmdline = "python mrq/scripts/mrq-worker.py %s high default low" % kwargs.get("flags", "")

    ProcessFixture.start(self, cmdline=cmdline, env=kwargs.get("env"))

    # This is a local worker instance that should never be started but used for launching tasks.
    self.local_worker = Worker(get_config(sources=("env")))
    self.local_worker.connect()

    self.mongodb_jobs = self.local_worker.mongodb_jobs
    self.mongodb_logs = self.local_worker.mongodb_logs
    self.redis = self.local_worker.redis

  def stop(self, **kwargs):

    ProcessFixture.stop(self, **kwargs)

    self.fixture_mongodb.stop(**kwargs)
    self.fixture_redis.stop(**kwargs)

  def send_tasks(self, path, params_list, block=True, queue=None, accept_statuses=["success"]):
    if not self.started:
      self.start()

    job_ids = send_tasks(path, params_list, queue=queue)

    if not block:
      return job_ids

    results = []

    for job_id in job_ids:
      job = wait_for_job(job_id, poll_interval=0.01)
      assert job.get("status") in accept_statuses

      results.append(job.get("result"))

    return results

  def send_task(self, path, params, **kwargs):
    return self.send_tasks(path, [params], **kwargs)[0]


@pytest.fixture(scope="function")
def httpstatic(request):
  return ProcessFixture(request, "/usr/sbin/nginx -c /app/tests/fixtures/httpstatic/nginx.conf", wait_port=8081)


@pytest.fixture(scope="function")
def mongodb(request):
  return ProcessFixture(request, "mongod", wait_port=27017, quiet=True)


@pytest.fixture(scope="function")
def redis(request):
  return ProcessFixture(request, "redis-server --save ''", wait_port=6379, quiet=True)


@pytest.fixture(scope="function")
def worker(request, mongodb, redis):

  return WorkerFixture(request, mongodb=mongodb, redis=redis)

