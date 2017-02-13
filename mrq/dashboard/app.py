from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from future.utils import iteritems
from gevent import monkey
monkey.patch_all()

from flask import Flask, request, render_template

import time
import os
import sys
import re
from bson import ObjectId
import json
import argparse
from werkzeug.serving import run_simple

sys.path.insert(0, os.getcwd())

from mrq.queue import Queue
from mrq.context import connections, set_current_config, get_current_config
from mrq.job import queue_job
from mrq.config import get_config

from mrq.dashboard.utils import jsonify, requires_auth

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser(description='Start the MRQ dashboard')

cfg = get_config(parser=parser, config_type="dashboard", sources=("file", "env", "args"))
set_current_config(cfg)

app = Flask(
    "dashboard",
    static_folder=os.path.join(CURRENT_DIRECTORY, "static"),
    template_folder=os.path.join(CURRENT_DIRECTORY, "templates")
)

WHITELISTED_MRQ_CONFIG_KEYS = ["dashboard_autolink_repositories"]


@app.route('/')
@requires_auth
def root():
    return render_template("index.html", MRQ_CONFIG={
        k: v for k, v in iteritems(cfg) if k in WHITELISTED_MRQ_CONFIG_KEYS
    })


@app.route('/api/datatables/taskexceptions')
@requires_auth
def api_task_exceptions():
    stats = list(connections.mongodb_jobs.mrq_jobs.aggregate([
        {"$match": {"status": "failed"}},
        {"$group": {"_id": {"path": "$path", "exceptiontype": "$exceptiontype"},
                    "jobs": {"$sum": 1}}},
    ]))

    stats.sort(key=lambda x: -x["jobs"])
    start = int(request.args.get("iDisplayStart", 0))
    end = int(request.args.get("iDisplayLength", 20)) + start

    data = {
        "aaData": stats[start:end],
        "iTotalDisplayRecords": len(stats)
    }

    data["sEcho"] = request.args["sEcho"]

    return jsonify(data)


@app.route('/api/datatables/status')
@requires_auth
def api_jobstatuses():
    stats = list(connections.mongodb_jobs.mrq_jobs.aggregate([
        # https://jira.mongodb.org/browse/SERVER-11447
        {"$sort": {"status": 1}},
        {"$group": {"_id": "$status", "jobs": {"$sum": 1}}}
    ]))

    stats.sort(key=lambda x: x["_id"])

    data = {
        "aaData": stats,
        "iTotalDisplayRecords": len(stats)
    }

    data["sEcho"] = request.args["sEcho"]

    return jsonify(data)


@app.route('/api/datatables/taskpaths')
@requires_auth
def api_taskpaths():
    stats = list(connections.mongodb_jobs.mrq_jobs.aggregate([
        {"$sort": {"path": 1}},  # https://jira.mongodb.org/browse/SERVER-11447
        {"$group": {"_id": "$path", "jobs": {"$sum": 1}}}
    ]))

    stats.sort(key=lambda x: -x["jobs"])

    data = {
        "aaData": stats,
        "iTotalDisplayRecords": len(stats)
    }

    data["sEcho"] = request.args["sEcho"]

    return jsonify(data)


@app.route('/workers')
@requires_auth
def get_workers():
    collection = connections.mongodb_jobs.mrq_workers
    cursor = collection.find({"status": {"$ne": "stop"}})
    data = {"workers": list(cursor)}
    return jsonify(data)


def build_api_datatables_query(req):
    query = {}

    if req.args.get("redisqueue"):
        query["_id"] = {"$in": [ObjectId(x) for x in Queue(
            req.args.get("redisqueue")).list_job_ids(limit=1000)]}
    else:

        for param in ["queue", "path", "exceptiontype"]:
            if req.args.get(param):
                if "*" in req.args[param]:
                    regexp = "^%s$" % re.escape(req.args[param]).replace("*", ".*")
                    query[param] = re.compile(regexp)
                else:
                    query[param] = req.args[param]

        if req.args.get("status"):
            statuses = req.args["status"].split("-")
            if len(statuses) == 1:
                query["status"] = statuses[0]
            else:
                query["status"] = {"$in": statuses}
        if req.args.get("id"):
            query["_id"] = ObjectId(req.args.get("id"))
        if req.args.get("worker"):
            query["worker"] = ObjectId(req.args.get("worker"))

        if req.args.get("params"):
            try:
                params_dict = json.loads(req.args.get("params"))

                for key in params_dict:
                    query["params.%s" % key] = params_dict[key]
            except Exception as e:  # pylint: disable=broad-except
                print("Error will converting form JSON: %s" % e)

    return query


@app.route('/api/datatables/<unit>')
@requires_auth
def api_datatables(unit):

    # import time
    # time.sleep(5)

    collection = None
    sort = None
    skip = int(request.args.get("iDisplayStart", 0))
    limit = int(request.args.get("iDisplayLength", 20))
    with_mongodb_size = bool(request.args.get("with_mongodb_size"))

    if unit == "queues":

        queues = []
        for name in Queue.all_known():
            queue = Queue(name)

            jobs = None
            if with_mongodb_size:
                jobs = connections.mongodb_jobs.mrq_jobs.count({
                    "queue": name,
                    "status": request.args.get("status") or "queued"
                })

            q = {
                "name": name,
                "jobs": jobs,  # MongoDB size
                "size": queue.size(),  # Redis size
                "is_sorted": queue.is_sorted,
                "is_timed": queue.is_timed,
                "is_raw": queue.is_raw,
                "is_set": queue.is_set
            }

            if queue.is_sorted:
                raw_config = cfg.get("raw_queues", {}).get(name, {})
                q["graph_config"] = raw_config.get("dashboard_graph", lambda: {
                    "start": time.time() - (7 * 24 * 3600),
                    "stop": time.time() + (7 * 24 * 3600),
                    "slices": 30
                } if queue.is_timed else {
                    "start": 0,
                    "stop": 100,
                    "slices": 30
                })()
                if q["graph_config"]:
                    q["graph"] = queue.get_sorted_graph(**q["graph_config"])

            if queue.is_timed:
                q["jobs_to_dequeue"] = queue.count_jobs_to_dequeue()

            queues.append(q)

        queues.sort(key=lambda x: -((x["jobs"] or 0) + x["size"]))

        data = {
            "aaData": queues,
            "iTotalDisplayRecords": len(queues)
        }

    elif unit == "workers":
        fields = None
        query = {"status": {"$nin": ["stop"]}}
        collection = connections.mongodb_jobs.mrq_workers
        sort = [("datestarted", -1)]

        if request.args.get("showstopped"):
            query = {}

    elif unit == "scheduled_jobs":
        collection = connections.mongodb_jobs.mrq_scheduled_jobs
        fields = None
        query = {}

    elif unit == "jobs":

        fields = None
        query = build_api_datatables_query(request)
        sort = [("_id", 1)]

        # We can't search easily params because we store it as decoded JSON in mongo :(
        # Add a string index?
        # if request.args.get("sSearch"):
        #   query.update(json.loads(request.args.get("sSearch")))
        collection = connections.mongodb_jobs.mrq_jobs

    if collection is not None:

        cursor = collection.find(query, projection=fields)

        if sort:
            cursor.sort(sort)

        if skip is not None:
            cursor.skip(skip)

        if limit is not None:
            cursor.limit(limit)

        data = {
            "aaData": list(cursor),
            "iTotalDisplayRecords": collection.find(query).count()
        }

    data["sEcho"] = request.args["sEcho"]

    return jsonify(data)


@app.route('/api/job/<job_id>/result')
@requires_auth
def api_job_result(job_id):
    collection = connections.mongodb_jobs.mrq_jobs

    job_data = collection.find_one(
        {"_id": ObjectId(job_id)}, projection=["result"])
    if not job_data:
        return jsonify({})

    return jsonify({
        "result": job_data.get("result")
    })


@app.route('/api/job/<job_id>/traceback')
@requires_auth
def api_job_traceback(job_id):
    collection = connections.mongodb_jobs.mrq_jobss
    if get_current_config().get("save_traceback_history"):

        field_sent = "traceback_history"
    else:
        field_sent = "traceback"

    job_data = collection.find_one(
        {"_id": ObjectId(job_id)}, projection=[field_sent])

    if not job_data:
        # If a job has no traceback history, we fallback onto traceback
        if field_sent == "traceback_history":
            field_sent = "traceback"
            job_data = collection.find_one(
                {"_id": ObjectId(job_id)}, projection=[field_sent])
        if not job_data:
            job_data = {}

    return jsonify({
        field_sent: job_data.get(field_sent, "No exception raised")
    })


@app.route('/api/jobaction', methods=["POST"])
@requires_auth
def api_job_action():
    params = {k: v for k, v in iteritems(request.form)}
    if params.get("status") and "-" in params.get("status"):
        params["status"] = params.get("status").split("-")
    return jsonify({"job_id": queue_job("mrq.basetasks.utils.JobAction",
                                        params,
                                        queue=get_current_config()["dashboard_queue"])})


@app.route('/api/logs')
@requires_auth
def api_logs():
    collection = connections.mongodb_logs.mrq_logs

    if request.args.get("job"):
        query = {"job": ObjectId(request.args.get("job"))}
    elif request.args.get("worker"):
        query = {"worker": ObjectId(request.args.get("worker"))}
    else:
        raise Exception("No ID")

    if request.args.get("last_log_id"):
        query["_id"] = {"$gt": ObjectId(request.args.get("min_log_id"))}

    logs = list(collection.find(query, projection={"_id": 1, "logs": 1}))

    data = {
        "logs": "\n".join([lines["logs"] for lines in logs]),

        # Don't be surprised, this will send unexisting ObjectIds when we're up
        # to date!
        "last_log_id": logs[-1]["_id"] if len(logs) else ObjectId()
    }

    return jsonify(data)


def main():
    app.debug = True
    run_simple(cfg["dashboard_ip"], int(cfg["dashboard_port"]), app)


if __name__ == '__main__':
    main()
