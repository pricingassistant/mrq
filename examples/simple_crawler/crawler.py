import requests
import lxml.html
import datetime
import re
import urlparse
from mrq.context import connections, log
from mrq.job import queue_job
from mrq.task import Task
from mrq.queue import Queue


class Fetch(Task):

    def run(self, params):

        collection = connections.mongodb_jobs.simple_crawler_urls

        response = requests.get(params["url"])

        if response.status_code != 200:
            log.warning("Got status %s on page %s (Queued from %s)" % (
                response.status_code, response.url, params.get("from")
            ))
            return False

        # Store redirects
        if response.url != params["url"]:
            collection.update({"_id": params["url"]}, {"$set": {
                "redirected_to": response.url,
                "fetched_date": datetime.datetime.now()
            }})

        document = lxml.html.fromstring(response.content)

        document.make_links_absolute(response.url)

        queued_count = 0

        document_domain = urlparse.urlparse(response.url).netloc

        for (element, attribute, link, pos) in document.iterlinks():

            link = re.sub("#.*", "", link or "")

            if not link:
                continue

            domain = urlparse.urlparse(link).netloc

            # Don't follow external links for this example
            if domain != document_domain:
                continue

            # We don't want to re-queue URLs twice. If we try to insert a duplicate,
            # pymongo will throw an error
            try:
                collection.insert({"_id": link})
            except:
                continue

            queue_job("crawler.Fetch", {
                "url": link,
                "from": params["url"]
            }, queue="crawl")
            queued_count += 1

        stored_data = {
            "_id": response.url,
            "queued_urls": queued_count,
            "html_length": len(response.content),
            "fetched_date": datetime.datetime.now()
        }

        collection.update(
            {"_id": response.url},
            stored_data,
            upsert=True
        )

        return True


class Report(Task):

    def run(self, params):

        collection = connections.mongodb_jobs.simple_crawler_urls

        print()
        print( "Crawl stats")
        print( "===========")
        print( "URLs queued: %s" % collection.find().count())
        print( "URLs successfully crawled: %s" % collection.find({"fetched_date": {"$exists": True}}).count())
        print( "URLs redirected: %s" % collection.find({"redirected_to": {"$exists": True}}).count())
        print( "Bytes fetched: %s" % (list(collection.aggregate(
            {"$group": {"_id": None, "sum": {"$sum": "$html_length"}}}
        )) or [{}])[0].get("sum", 0))
        print()


class Reset(Task):

    def run(self, params):

        collection = connections.mongodb_jobs.simple_crawler_urls

        collection.remove({})

        Queue("crawl").empty()
