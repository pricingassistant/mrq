from mrq.logger import log
from mrq.queue import send_task
import datetime


class Scheduler(object):

  def __init__(self, collection):
    self.collection = collection

    self.refresh()

  def refresh(self):
    self.all_tasks = self.collection.find()

  def hash_task(self, task):

    return "%s %s %s" % (task["path"], task["params"], task.get("interval", 0))

  def sync_tasks(self, tasks):
    """ Performs the first sync of a list of tasks, often defined in the config file. """

    tasks_by_hash = {self.hash_task(t): t for t in tasks}

    for task in self.all_tasks:
      if tasks_by_hash.get(task["hash"]):
        del tasks_by_hash[task["hash"]]
      else:
        self.collection.remove({"_id": task["_id"]})
        log.info("Scheduler: deleted %s" % task["hash"])

    for h, task in tasks_by_hash.iteritems():
      task["hash"] = h
      self.collection.insert(task)
      log.info("Scheduler: added %s" % task["hash"])

      # # Ajust the time
      # if "dailytime" in SCHEDULE[k]:
      #   now = datetime.utcnow()
      #   if now.time() < SCHEDULE[k]["dailytime"]:
      #     newtime = datetime.combine(now.date(), SCHEDULE[k]["dailytime"])
      #   else:
      #     newtime = datetime.combine(now.date() + timedelta(hours=24), SCHEDULE[k]["dailytime"])

      #   print "Updating execution time of %s to %s" % (k, newtime)
      #   scheduler.change_execution_time(job, newtime)

  def check(self):

    for task in self.all_tasks:

      next_time = task.get("datelastqueued", datetime.datetime.fromtimestamp(0)) + datetime.timedelta(seconds=task["interval"])

      task_data = self.collection.find_and_modify({
        "_id": task["_id"],
        "datelastqueued": {"$gte": next_time}
      }, {"$set": {
        "datelastqueued": datetime.datetime.utcnow()
      }})

      if task_data:
        send_task(task_data["path"], task_data["params"])
        log.info("Scheduler: queued %s" % task_data)

        self.refresh()
