# Raw queues

With regular queues, MRQ stores the task metadata in MongoDB and the task IDs in a Redis list. This design allows a good compromise between performance and visibility.

Raw queues give you more performance and some new features in exchange for a bit less visibility. In their case, only the parameters of a task are stored in serialized form in Redis when queued, and they are inserted in MongoDB only after being dequeued by a worker.

There are 4 types of raw queues. The type of a queue is determined by a suffix in its name:

 * ```_raw``` : The simpliest raw queue, stored in a Redis LIST.
 * ```_set``` : Stored in a Redis SET. Gives you the ability to have "unique" tasks: only one (task, parameters) couple can be queued at a time.
 * ```_sorted_set``` : The most powerful MRQ queue type, stored in a Redis ZSET. Allows you to order (and re-order) the tasks to be dequeued. Like ```_set```, task parameters will be unique.
 * ```_timed_set``` : A special case of ```_sorted_set```, where tasks are sorted with a UNIX timestamp. This means you can schedule tasks to be executed at a precise time in the future.

Raw queues need a special entry in the configuration to unserialize their "raw" parameters in a regular dict of parameters. They also need to be linked to a regular queue ("default" if none) for providing visibility and retries, after they are dequeued from the raw queue.

This is an example of raw queue configuration:

```python
RAW_QUEUES = {
  "myqueue_raw": {
      "job_factory": lambda rawparam: {
          "path": "tests.tasks.general.Add",
          "params": {
              "a": int(rawparam.split(" ")[0]),
              "b": int(rawparam.split(" ")[1])
          }
      },
      "retry_queue": "high"
  }
}
```

This task adds two integers. To queue tasks, you can do from the code:

```python
from mrq.queue import send_raw_tasks

send_raw_tasks("myqueue_raw", [
  ["1 1"],
  ["42 8"]
])
```

To run them, start a worker listening to both queues:

```
$ mrq-worker high myqueue_raw
```

Queueing on timed sets is a bit different, you can pass unix timestamps directly:

```python
from mrq.queue import send_raw_tasks
import time

send_raw_tasks("myqueue_timed_set", {
  "rawparam_xxx": time.time(),
  "rawparam_yyy": time.time() + 3600  # Do this in an hour
})
```

For more examples of raw queue configuration, check https://github.com/pricingassistant/mrq/blob/master/tests/fixtures/config-raw1.py
