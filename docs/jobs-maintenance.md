# Jobs maintenance

MRQ can provide strong guarantees that no job will be lost in the middle of a worker restart, database disconnect, etc...

To do that, you should add these recurring scheduled jobs to your mrq-config.py:

```
SCHEDULER_TASKS = [

  # This will queue jobs in the 'delayed' status.
  {
    "path": "mrq.basetasks.cleaning.QueueDelayedJobs",
    "params": {},
    "interval": 60
  },

  # This will requeue jobs in the 'retry' status, until they reach their max_retries.
  {
    "path": "mrq.basetasks.cleaning.RequeueRetryJobs",
    "params": {},
    "interval": 60
  },

  # This will requeue jobs marked as interrupted, for instance when a worker received SIGTERM
  {
    "path": "mrq.basetasks.cleaning.RequeueInterruptedJobs",
    "params": {},
    "interval": 5 * 60
  },

  # This will requeue jobs marked as started for a long time (more than their own timeout)
  # They can exist if a worker was killed with SIGKILL and not given any time to mark
  # its current jobs as interrupted.
  {
    "path": "mrq.basetasks.cleaning.RequeueStartedJobs",
    "params": {},
    "interval": 3600
  },

  # This will make sure MRQ's indexes are built
  {
    "path": "mrq.basetasks.indexes.EnsureIndexes",
    "params": {},
    "interval": 24 * 3600
  }
]
```

Obviously this implies that all your jobs should be *idempotent*, meaning that they could be done multiple times, maybe partially, without breaking your app. This is a very good design to enforce for your whole task queue, though you can still manage locks yourself in your code that make sure a block of code will only run once.
