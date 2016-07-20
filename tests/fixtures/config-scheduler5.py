import datetime
import os

SCHEDULER_INTERVAL = 1
SCHEDULER_TASKS = []

for i in range(7):
  SCHEDULER_TASKS.extend([
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "weekday": i,
            "later": False
        },
        "weekday": i,
        "dailytime": datetime.datetime.fromtimestamp(float(os.environ.get("MRQ_TEST_SCHEDULER_TIME"))).time()
    },
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "weekday": i,
            "later": True
        },
        "weekday": i,
        "dailytime": datetime.datetime.fromtimestamp(float(os.environ.get("MRQ_TEST_SCHEDULER_TIME")) + 1000).time()
    }])
