NAME = "testworker"

TASKS = {
    "tests.tasks.general.Retry": {
        "default_ttl": 2,
        "queue": "tests"
    }
}

QUEUES = ["high", "default", "low", "tests"]
