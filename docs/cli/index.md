
# Commands

All the command-line tools support a set of common configuration flags, defined in [config.py](https://github.com/pricingassistant/mrq/blob/master/mrq/config.py). Use --help with any of them to see the full list.

 - `mrq-worker` starts a worker
 - `mrq-dashboard` starts the web dashboard on the default port
 - `mrq-run` runs a task. If you add the `--async` option that will enqueue it to be later ran by a worker

Typical usage is:
```
$ mrq-run tasks.mylib.myfile.MyTask '{"param1": 1, "param2": True}'
```