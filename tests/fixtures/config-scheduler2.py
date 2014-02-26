
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
      "a": 20
    },
    "interval": 5
  },
  {
    "path": "mrq.basetasks.tests.general.MongoInsert",
    "params": {
      "a": 3
    },
    "interval": 10
  },
  {
    "path": "mrq.basetasks.tests.general.MongoInsert2",
    "params": {
      "a": 4
    },
    "interval": 5
  }
]

SCHEDULER_INTERVAL = 1
