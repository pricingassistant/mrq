from .logger import LoggerInterface
import gevent

# greenletid => Job object
_GREENLET_JOBS_REGISTRY = {}

_WORKER = None

# Global log object, usable from all tasks
log = LoggerInterface(None, job="current")


def set_current_job(job):
  _GREENLET_JOBS_REGISTRY[id(gevent.getcurrent())] = job


def get_current_job(greenlet_id=None):
  if greenlet_id is None:
    greenlet_id = id(gevent.getcurrent())
  return _GREENLET_JOBS_REGISTRY.get(greenlet_id)


def set_current_worker(worker):
  global _WORKER
  _WORKER = worker


def get_current_worker():
  global _WORKER
  return _WORKER


def retry_current_job(**kwargs):
  get_current_job().retry(**kwargs)
