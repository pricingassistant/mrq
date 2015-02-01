# Configuration

For each of these values, configuration is loaded in this order by default:

- Command-line arguments (`mrq-worker --redis=redis://127.0.0.1:6379`)
- Environment variables prefixed by MRQ_ (`MRQ_REDIS=redis://127.0.0.1:6379 mrq-worker`)
- Python variables in a config file, by default `mrq-config.py` (`REDIS="redis://127.0.0.1:6379"` in this file)

Most of the time, you want to set all your configuration in a `mrq-config.py` file in the directory where you will launch your workers, and override some of it from the command line.

On Heroku, environment variables are very handy because they can be set like `heroku config:set MRQ_REDIS=redis://127.0.0.1:6379`

## Tasks configuration

Tasks can alter some default values of their jobs. These values can be configured in the config file like this (values below are :

```python

TASKS = {
    "tasks.MyTask": {

        # In seconds, delay before interrupting the job
        "timeout": 3600,

        # Default queue when none is specified in queue_job()
        "queue": "default",

        # Set the status to "maxretries" after retrying that many times
        "max_retries": 3,

        # Seconds before a job in retry status is requeued again
        "retry_delay": 600,

        # Keep jobs with status in ("success", "cancel", "abort") that many seconds
        # in MongoDB
        "result_ttl": 7 * 24 * 3600,

    }
}

```