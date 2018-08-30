RAW_QUEUES = {
  "example_timed_set": {
    "job_factory": lambda rawparam: {
      "path": "example.Print",
      "params": {
        "test": rawparam
      }
    }
  }
}