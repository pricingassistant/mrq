import time

RAW_QUEUES = {
    "pushback_timed_set": {
        "pushback_seconds": 24 * 3600,
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.MongoInsert",
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
            "path": "tests.tasks.general.MongoInsert",
            "params": {
                "timed_set": rawparam
            }
        }
    },
    "test_raw": {
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.MongoInsert",
            "params": {
                "raw": rawparam
            }
        }
    },
    "test_set": {
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.MongoInsert",
            "params": {
                "set": rawparam
            }
        }
    },
    "test_sorted_set": {
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.MongoInsert",
            "params": {
                "sorted_set": rawparam
            }
        }
    },
    "testexception_raw": {
        "retry_queue": "testx",
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.RaiseException",
            "params": {
                "message": rawparam
            }
        }
    },
    "testretry_raw": {
        "retry_queue": "testx",
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.Retry",
            "params": {
                "sleep": int(rawparam),
                "delay": 0
            }
        }
    },
    "testperformance_raw": {
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.Add",
            "params": {
                "a": int(rawparam),
                "b": 0,
                "sleep": 0
            }
        }
    },
    "testperformance_set": {
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.Add",
            "params": {
                "a": int(rawparam),
                "b": 0,
                "sleep": 0
            }
        }
    },
    "testperformance_timed_set": {
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.Add",
            "params": {
                "a": int(rawparam),
                "b": 0,
                "sleep": 0
            }
        }
    },
    "testperformance_sorted_set": {
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.Add",
            "params": {
                "a": int(rawparam),
                "b": 0,
                "sleep": 0
            }
        }
    },
    "teststarted_raw": {
        "retry_queue": "teststartedx",
        "job_factory": lambda rawparam: {
            "path": "tests.tasks.general.WaitForFlag",
            "params": {
                "flag": rawparam
            }
        }
    },
    "testnostorage_raw": {
        "retry_queue": "testnostorage",
        "statuses_no_storage": ("started", "success"),
        "job_factory": lambda rawparam: {
            "path": rawparam.split(" ")[0],
            "params": {
                "sleep": float(rawparam.split(" ")[1])
            }
        }
    }
}
