from future.builtins import str, object
from .context import log, queue_job
import datetime
import ujson as json
import time


def _hash_task(task):
    """ Returns a unique hash for identify a task and its params """

    params = task.get("params")
    if params:
        params = json.dumps(sorted(list(task["params"].items()), key=lambda x: x[0]))  # pylint: disable=no-member

    full = [str(task.get(x)) for x in ["path", "interval", "dailytime", "weekday", "monthday", "queue"]]

    full.extend([str(params)])
    return " ".join(full)


class Scheduler(object):

    def __init__(self, collection, config_tasks):
        self.collection = collection
        self.config_tasks = config_tasks
        self.config_synced = False
        self.all_tasks = []

    def check_config_integrity(self):
        """ Make sure the scheduler config is valid """
        tasks_by_hash = {_hash_task(t): t for t in self.config_tasks}

        if len(tasks_by_hash) != len(self.config_tasks):
            raise Exception("Fatal error: there was a hash duplicate in the scheduled tasks config.")

        for h, task in tasks_by_hash.items():
            if task.get("monthday") and not task.get("dailytime"):
                raise Exception("Fatal error: you can't schedule a task with 'monthday' and without 'dailytime' (%s)" % h)
            if task.get("weekday") and not task.get("dailytime"):
                raise Exception("Fatal error: you can't schedule a task with 'weekday' and without 'dailytime' (%s)" % h)

            if not task.get("monthday") and not task.get("weekday") and not task.get("dailytime") and not task.get("interval"):
                raise Exception("Fatal error: scheduler must be specified one of monthday,weekday,dailytime,interval. (%s)" % h)

    def sync_config_tasks(self):
        """ Performs the first sync of a list of tasks, often defined in the config file. """

        tasks_by_hash = {_hash_task(t): t for t in self.config_tasks}

        for task in self.all_tasks:
            if tasks_by_hash.get(task["hash"]):
                del tasks_by_hash[task["hash"]]
            else:
                self.collection.remove({"_id": task["_id"]})
                log.debug("Scheduler: deleted %s" % task["hash"])

        # What remains are the new ones to be inserted
        for h, task in tasks_by_hash.items():
            task["hash"] = h
            task["datelastqueued"] = datetime.datetime.fromtimestamp(0)
            if task.get("dailytime"):
                # Because MongoDB can store datetimes but not times,
                # we add today's date to the dailytime.
                # The date part will be discarded in check()
                task["dailytime"] = datetime.datetime.combine(
                    datetime.datetime.utcnow(), task["dailytime"])
                task["interval"] = 3600 * 24

                # Avoid to queue task in check() if today dailytime is already passed
                if datetime.datetime.utcnow().time() > task["dailytime"].time():
                    task["datelastqueued"] = datetime.datetime.utcnow()

            self.collection.find_one_and_update({"hash": task["hash"]}, {"$set": task}, upsert=True)
            log.debug("Scheduler: added %s" % task["hash"])

    def check(self):

        self.all_tasks = list(self.collection.find())

        if not self.config_synced:
            self.sync_config_tasks()
            self.all_tasks = list(self.collection.find())
            self.config_synced = True

        # log.debug(
        #     "Scheduler checking for out-of-date scheduled tasks (%s scheduled)..." %
        #     len(self.all_tasks)
        # )

        now = datetime.datetime.utcnow()
        current_weekday = now.weekday()
        current_monthday = now.day

        for task in self.all_tasks:

            interval = datetime.timedelta(seconds=task["interval"])

            if task["datelastqueued"] >= now:
                continue

            if task.get("monthday", current_monthday) != current_monthday:
                continue

            if task.get("weekday", current_weekday) != current_weekday:
                continue

            if task.get("dailytime"):
                if task["datelastqueued"].date() == now.date() or now.time() < task["dailytime"].time():
                    continue

            # if we only have "interval" key
            if all(k not in task for k in ["monthday", "weekday", "dailytime"]):
                if now - task["datelastqueued"] < interval:
                    continue

            queue_job(
                task["path"],
                task.get("params") or {},
                queue=task.get("queue")
            )

            self.collection.update({"_id": task["_id"]}, {"$set": {
                "datelastqueued": now
            }})

            log.debug("Scheduler: queued %s" % _hash_task(task))

        # Make sure we never again execute a scheduler with the same exact second.
        time.sleep(1)
