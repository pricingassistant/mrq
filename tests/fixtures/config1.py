NAME = "testworker"

TASKS = {
  "mrq.basetasks.tests.general.TimeoutFromConfig": {
    "timeout": 2,
    "queue": "tests"
  }
}

from datetime import timedelta

SCHEDULED_TASKS = [
  {
    "path": "mrq.basetasks.tests.general.TimeoutFromConfig",
    "params": {
      "a": 1,
      "b": 2
    },
    "interval": timedelta(seconds=5).total_seconds()
  }
]

QUEUES = ["high", "default", "low", "tests"]
