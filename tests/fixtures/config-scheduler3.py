import datetime

SCHEDULER_TASKS = [
  {
    "path": "mrq.basetasks.tests.general.MongoInsert",
    "params": {
      "a": 1
    },
    "dailytime": (datetime.datetime.utcnow() + datetime.timedelta(seconds=3)).time()
  },
]

SCHEDULER_INTERVAL = 1
