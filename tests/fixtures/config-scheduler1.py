
SCHEDULER_TASKS = [
  {
    "path": "mrq.basetasks.tests.general.MongoInsert",
    "params": {
      "a": 1
    },
    "interval": 5
  },
  {
    "path": "mrq.basetasks.tests.general.MongoInsert",
    "params": {
      "a": 2
    },
    "interval": 5
  },
  {
    "path": "mrq.basetasks.tests.general.MongoInsert",
    "params": {
      "a": 3
    },
    "interval": 5
  },
  {
    "path": "mrq.basetasks.tests.general.MongoInsert",
    "params": {
      "a": 4
    },
    "interval": 5
  }
]

SCHEDULER_INTERVAL = 1
