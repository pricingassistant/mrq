# Metrics & Graphite

MRQ doesn't support sending metrics to Graphite out of the box but makes it extremely easy to do so.

All you have to do is add this hook in your mrq-config file:

```python
import graphiteudp  # Install this via pip

# Initialize the Graphite UDP Client
_graphite_client = graphiteudp.GraphiteUDPClient(host, port, prefix, debug=False)
_graphite_client.init()

def METRIC_HOOK(name, incr=1, **kwargs):

  # You can use this to avoid sending too many different metrics
  whitelisted_metrics = ["queues.all.", "queues.default.", "jobs."]

  if any([name.startswith(m) for m in whitelisted_metrics]):
    _graphite_client.send(name, incr)
```

If you have another monitoring system you can plug anything in this hook to connect to it!