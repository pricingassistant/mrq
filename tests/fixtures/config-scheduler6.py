import datetime
import os

SCHEDULER_TASKS = [
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "monthday": i + 1
        },
        "monthday": i + 1,
        "dailytime": datetime.datetime.fromtimestamp(float(os.environ.get("MRQ_TEST_SCHEDULER_TIME"))).time()
    } for i in range(31)
]

SCHEDULER_INTERVAL = 1
