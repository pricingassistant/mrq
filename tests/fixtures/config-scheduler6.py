import datetime
import os

SCHEDULER_TASKS = [
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "monthday": i
        },
        "monthday": i,
        "dailytime": datetime.datetime.fromtimestamp(float(os.environ.get("MRQ_TEST_SCHEDULER_TIME"))).time()
    } for i in range(31)
]

SCHEDULER_INTERVAL = 1
