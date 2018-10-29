import datetime
import os

SCHEDULER_TASKS = [
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "monthday": i
        },
        "monthday": i
    } for i in range(7)
]

SCHEDULER_INTERVAL = 1
