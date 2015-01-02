NAME = "testworker"

TASKS = {
    "tests.tasks.general.TimeoutFromConfig": {
        "timeout": 2,
        "queue": "tests"
    }
}

QUEUES = ["high", "default", "low", "tests"]
