NAME = "testworker"

TASKS = {
  "mrq.basetasks.tests.general.TimeoutFromConfig": {
    "timeout": 2,
    "queue": "tests"
  },
  "mrq.basetasks.tests.general.TimeoutFromConfigAndCancel": {
    "timeout": 2,
    "queue": "tests"
  }
}

QUEUES = ["high", "default", "low", "tests"]
