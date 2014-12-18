# mrq-worker

## Concurrency

The default is to run tasks one at a time. You should obviously change this behaviour to use Gevent's full capabilities with something like:

`mrq-worker --processes 3 --gevent 10`

This will start 30 greenlets over 3 UNIX processes. Each of them will run 10 jobs at the same time.

As soon as you use the `--processes` option (even with `--processes=1`) then supervisord will be used to control the processes. It is quite useful to manage long-running instances.


## Simulating network latency

Sometimes it is helpful in local development to simulate an environment with higher network latency.

To do this we added a ```--add_network_latency=0.1``` config option that will add (in this case) a random delay between 0 and 0.1 seconds to every network call.