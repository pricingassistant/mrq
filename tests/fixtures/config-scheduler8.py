NAME = "testworker"

TASKS = {
    "tests.tasks.general.Retry": {
        "default_ttl": 2 * 24 * 3600,
        "queue": "tests"
    }
}

QUEUES = ["high", "default", "low", "tests"]
