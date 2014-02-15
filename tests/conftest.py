import pytest
import os
import subprocess
import sys

sys.path.append(os.getcwd())

from mrq.worker import Worker
from mrq.queue import send_tasks, wait_for_result
from mrq.config import get_config
from mrq.utils import wait_for_net_service


class ProcessFixture(object):
  def __init__(self, request, cmdline=None, wait_port=None):
    self.request = request
    self.cmdline = cmdline
    self.process = None
    self.wait_port = wait_port

    self.request.addfinalizer(self.stop)

  def start(self, cmdline=None, env=None):
    if not cmdline:
      cmdline = self.cmdline
    if env is None:
      env = {}

    self.process = subprocess.Popen(cmdline.split(" ") if type(cmdline) in [str, unicode] else cmdline, shell=False, close_fds=True, env=env, cwd=os.getcwd())

    if self.wait_port:
      wait_for_net_service("127.0.0.1", int(self.wait_port))

  def stop(self):
    if self.process is not None:
      print "kill -2 %s" % self.process.pid
      os.kill(self.process.pid, 2)


class WorkerFixture(ProcessFixture):

  def __init__(self, request, **kwargs):
    ProcessFixture.__init__(self, request, cmdline=kwargs.get("cmdline"))

    self.mongodb = kwargs["mongodb"]
    self.redis = kwargs["redis"]

    self.started = False

  def start(self, **kwargs):

    self.mongodb.start()
    self.redis.start()

    ProcessFixture.start(self, **kwargs)

    # This is a local worker instance that should never be started but used for launching tasks.
    self.local_worker = Worker(get_config(sources=("env")))

  def send_tasks(self, path, params_list, block=True, queue=None):
    if not self.started:
      self.start()

    job_ids = send_tasks(path, params_list, queue=queue)

    if not block:
      return job_ids

    results = [wait_for_result(job_id) for job_id in job_ids]

    return results

  def send_task(self, path, params, **kwargs):
    return self.send_tasks(path, [params], **kwargs)[0]


@pytest.fixture(scope="function")
def mongodb(request):
  return ProcessFixture(request, "mongod", wait_port=27017)


@pytest.fixture(scope="function")
def redis(request):
  return ProcessFixture(request, "redis-server", wait_port=6379)


@pytest.fixture(scope="function")
def worker(request, mongodb, redis):

  return WorkerFixture(request, cmdline="python mrq/scripts/mrqworker.py high default low", mongodb=mongodb, redis=redis)

