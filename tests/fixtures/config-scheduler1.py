
SCHEDULER_TASKS = [
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "a": 1
        },
        "interval": 5
    },
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "a": 2
        },
        "interval": 5
    },
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "a": 3
        },
        "interval": 5
    },
    {
        "path": "tests.tasks.general.MongoInsert",
        "params": {
            "a": 4
        },
        "interval": 5
    }
]

SCHEDULER_INTERVAL = 1
