The following is a list of recommendations that you should strongly consider in order to benefit from MRQ's design.

## Reentrant tasks

From [Wikipedia](https://en.wikipedia.org/wiki/Reentrancy_(computing)):

> A subroutine is called reentrant if it can be interrupted in the middle of its execution and then safely called again ("re-entered") before its previous invocations complete execution.

This is a desired property of all tasks because by nature, code execution can be interrupted at any time. One single worker should always be considered unreliable. MRQ's job is to work around this issue by automatically requeueing interrupted jobs (see [Jobs maintenance](jobs-maintenance.md))

A good real-world example of this is the fact that Heroku dynos are restarted at least once a day.

## Monitoring failed jobs

There are multiple, often unpredictable reasons jobs can fail and developer teams should monitor the list of failed tasks at all times.

MRQ's Dashboard provides a dedicated view for failed tasks, conveniently grouped by Exception.

## Using the retry status

All Exceptions shouldn't cause a failed job. For instance, doing an HTTP request might fail and most of the time, you will want to retry it at a later time. This is why it is useful to wrap code that can potentially raise an Exception in a try/except block, and call `retry_current_job` instead, like this:

```python
from mrq.task import Task
from mrq.context import retry_current_job, log
import urllib2

class SafeFetchTask(Task):
  def run(self, params):

    try:
      with urllib2.urlopen(params["url"]) as f:
        t = f.read()
        return len(t)

    except urllib2.HTTPError, e:
      log.warning("Got HTTP error %s, retrying...", e)
      retry_current_job()
```

Remember to add the base recurring jobs as explained in [Jobs maintenance](jobs-maintenance.md) to have `retry` jobs actuallt requeued.

## Using your own base Task class

MRQ provides sensible defaults but in many cases, you will want to customize its behaviour and API.

We recommend subclassing `mrq.task.Task` with your own base Task class, and have all your tasks subclass it instead. The `run_wrapped` method is a convenient entry point to wrap all further code.

This is a simple example of a somewhat useful class `BaseTask`:

```python
from mrq.task import Task
from mrq.context import retry_current_job
import urllib2

class BaseTask(Task):

  retry_on_http_error = True

  def validate_params(self, params):
    """ Make sure some standard parameters are well-formatted """
    if "url" in params:
      assert "://" in params["url"]

  def run_wrapped(self, params):
    """ Wrap all calls to tasks in init & safety code. """

    self.validate_params(params)

    try:
      return self.run(params)

    # Intercept HTTPErrors in all tasks and retry them by default
    except urllib2.HTTPError, e:
      if self.retry_on_http_error:
        retry_current_job()

```