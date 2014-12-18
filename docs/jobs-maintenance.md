# Jobs maintenance

MRQ can provide strong guarantees that no job will be lost in the middle of a worker restart, database disconnect, etc...

To do that, you should add these recurring scheduled jobs to your mrq-config.py:

```
SCHEDULER_TASKS = [

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

  # This will requeue jobs 'lost' between redis.blpop() and mongo.update(status=started).
  # This can happen only when the worker is killed brutally in the middle of dequeue_jobs()
  {
    "path": "mrq.basetasks.cleaning.RequeueLostJobs",
    "params": {},
    "interval": 24 * 3600
  }
]
```

Obviously this implies that all your jobs should be *idempotent*, meaning that they could be done multiple times, maybe partially, without breaking your app. This is a very good design to enforce for your whole task queue, though you can still manage locks yourself in your code that make sure a block of code will only run once.
