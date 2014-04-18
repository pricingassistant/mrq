RAW_QUEUES = {
  "test.timed": {
    "pushback_seconds": 24 * 3600,
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.MongoInsert",
      "params": {
        "rawp": rawparam
      }
    }
  }
}
