from .context import log
from .queue import send_task
import datetime


class Scheduler(object):

  def __init__(self, collection):
    self.collection = collection

    self.refresh()

  def refresh(self):
    self.all_tasks = list(self.collection.find())

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
        log.debug("Scheduler: deleted %s" % task["hash"])

    for h, task in tasks_by_hash.iteritems():
      task["hash"] = h
      task["datelastqueued"] = datetime.datetime.fromtimestamp(0)
      self.collection.insert(task)
      log.debug("Scheduler: added %s" % task["hash"])

    self.refresh()

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

    log.debug("Scheduler checking for out-of-date scheduled tasks (%s scheduled)..." % len(self.all_tasks))
    for task in self.all_tasks:

      interval = datetime.timedelta(seconds=task["interval"])

      last_time = datetime.datetime.utcnow() - interval

      task_data = self.collection.find_and_modify({
        "_id": task["_id"],
        "datelastqueued": {"$lt": last_time}
      }, {"$set": {
        "datelastqueued": datetime.datetime.utcnow()
      }})

      if task_data:
        send_task(task_data["path"], task_data["params"], queue=task.get("queue"))
        log.debug("Scheduler: queued %s" % task_data)

        self.refresh()
