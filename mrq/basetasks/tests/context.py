from mrq.task import Task
from mrq.context import log, get_current_job, get_current_worker, get_current_config


class GetContext(Task):
  def run(self, params):
    log.info("Getting context info...")
    return {
      "job_id": get_current_job().id,
      "worker_id": get_current_worker().id,
      "config": get_current_config()
    }
