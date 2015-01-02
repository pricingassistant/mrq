
## Worker statuses

A Worker is always in one of these statuses:

* `init`: General worker initialization
* `wait`: Waiting for new jobs from Redis
* `spawn`: Got some new jobs, greenlets are being spawned
* `full`: All the worker pool is busy executing jobs
* `join`: Waiting for current jobs to finish, no new one will be accepted
* `kill`: Killing all current jobs
* `stop`: Worker is stopped, no jobs should remain