import time

RAW_QUEUES = {
  "pushback_timed_set": {
    "pushback_seconds": 24 * 3600,
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.MongoInsert",
      "params": {
        "timed_set": rawparam
      }
    }
  },
  "test_timed_set": {
    "dashboard_graph": lambda: {
      "start": time.time() - (24 * 3600),
      "stop": time.time() + (24 * 3600),
      "slices": 30,
      "include_inf": True,
      "exact": False
    },
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.MongoInsert",
      "params": {
        "timed_set": rawparam
      }
    }
  },
  "test_raw": {
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.MongoInsert",
      "params": {
        "raw": rawparam
      }
    }
  },
  "test_set": {
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.MongoInsert",
      "params": {
        "set": rawparam
      }
    }
  },
  "test_sorted_set": {
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.MongoInsert",
      "params": {
        "sorted_set": rawparam
      }
    }
  },
  "testexception_raw": {
    "retry_queue": "testx",
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.RaiseException",
      "params": {
        "message": rawparam
      }
    }
  },
  "testretry_raw": {
    "retry_queue": "testx",
    "job_factory": lambda rawparam: {
      "path": "mrq.basetasks.tests.general.Retry",
      "params": {
        "sleep": int(rawparam),
        "countdown": 0
      }
    }
  },
}
