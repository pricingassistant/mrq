# Dependencies

## Python

MRQ has only been tested with Python 2.7+.

Required external services dependencies are

 - [MongoDB >= 2.4](http://docs.mongodb.org/manual/installation/)
 - [Redis >= 2.6](http://redis.io/topics/quickstart)

We use LUA scripting in Redis to boost performance and provide extra safety.

You will need [Docker](http://docker.io) to run our unit tests. Our [Dockerfile](https://github.com/pricingassistant/mrq/blob/master/Dockerfile) is actually a good way to see a complete list of dependencies, including dev tools like graphviz for memleak images.

You may want to convert your logs db to a capped collection : ie. run db.

```
runCommand({"convertToCapped": "mrq_jobs", "size": 10737418240})
```

## Javascript

JS libraries used in the [Dashboard](dashboard.md):

 * [BackboneJS](http://backbonejs.org)
 * [UnderscoreJS](http://underscorejs.org)
 * [RequireJS](http://requirejs.org)
 * [MomentJS](http://momentjs.com)
 * [jQuery](http://jquery.com)
 * [Datatables](http://datatables.net)
 * [Datatables-Bootstrap3](https://github.com/Jowin/Datatables-Bootstrap3/)
 * [Twitter Bootstrap](https://github.com/twbs/bootstrap)

# Credits

Inspirations:

 * [RQ](http://python-rq.org/)
 * [Celery](www.celeryproject.org)


# Useful third-party utils

 * http://superlance.readthedocs.org/en/latest/
