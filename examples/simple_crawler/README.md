Simple Web Crawler with MRQ
===========================

This is a simple demo app that crawls a website, to demo some MRQ features.


How to use
==========

First, get into the docker image at the root of this directory:
```
docker run -t -i -v `pwd`:/src -w /src pricingassistant/mrq bash
```

Then install MRQ and the packages needed for this example:
```
$ cd examples/simple_crawler
$ sudo apt-get install libxml2-dev libxslt1-dev libz-dev     # Needed for lxml
$ pip install -r requirements.txt
```

Launch MongoDB & Redis if they are not already started:
```
$ mongod &
$ redis-server &
```

Queue the first task via the command line:

```
$ mrq-run --queue crawl crawler.Fetch '{"url": "http://docs.python-requests.org/"}'
```

Then start a worker with 3 (or more!) greenlets:

```
$ mrq-worker crawl --greenlets 3
```

You should also launch a dashboard to monitor the progress:
```
$ mrq-dashboard
```

We also included 2 utility tasks:
```
$ mrq-run crawler.Report
$ mrq-run crawler.Reset
```

This is obviously a very simple crawler, production systems will be much more complex but it gives you an overview of MRQ and a good starting point.


Expected result for crawler.Report
==================================

As of 2018-03-06:

```
Crawl stats
===========
URLs queued: 81
URLs successfully crawled: 81
URLs redirected: 1
Bytes fetched: 4099658
```
