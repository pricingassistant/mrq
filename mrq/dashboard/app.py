from gevent import monkey
monkey.patch_all()

from flask import Flask, request

import time
import os
import sys
from bson import ObjectId
import json
import argparse
from gevent.pywsgi import WSGIServer
from werkzeug.serving import run_with_reloader

sys.path.insert(0, os.getcwd())

from mrq.queue import send_task, Queue
from mrq.context import connections, set_current_config, get_current_config
from mrq.config import get_config

from mrq.dashboard.utils import jsonify, requires_auth

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser(description='Start the MRQ dashboard')

cfg = get_config(parser=parser, config_type="dashboard")
set_current_config(cfg)

app = Flask("dashboard", static_folder=os.path.join(CURRENT_DIRECTORY, "static"))


@app.route('/')
@requires_auth
def root():
  return app.send_static_file("index.html")


@app.route('/api/datatables/taskexceptions')
@requires_auth
def api_task_exceptions():
  stats = list(connections.mongodb_jobs.mrq_jobs.aggregate([
    {"$match": {"status": "failed"}},
    {"$group": {"_id": {"path": "$path", "exceptiontype": "$exceptiontype"}, "jobs": {"$sum": 1}}},
  ])["result"])

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
    {"$sort": {"status": 1}},  # https://jira.mongodb.org/browse/SERVER-11447
    {"$group": {"_id": "$status", "jobs": {"$sum": 1}}}
  ])["result"])

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
  ])["result"])

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
    query["_id"] = {"$in": [ObjectId(x) for x in Queue(req.args.get("redisqueue")).list_job_ids(limit=1000)]}
  else:

    for param in ["queue", "path", "status", "exceptiontype"]:
      if req.args.get(param):
        query[param] = req.args.get(param)
    if req.args.get("id"):
      query["_id"] = ObjectId(req.args.get("id"))
    if req.args.get("worker"):
      query["worker"] = ObjectId(req.args.get("worker"))

    if req.args.get("params"):
      try:
        params_dict = json.loads(req.args.get("params"))

        for key in params_dict.keys():
          query["params.%s" % key] = params_dict[key]
      except Exception as e:
        print "Error will converting form JSON: %s" % e

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

  if unit == "queues":

    queues = []
    for name, jobs in Queue.all().items():
      queue = Queue(name)
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
        q["graph_config"] = cfg.get("raw_queues", {}).get(name, {}).get("dashboard_graph", lambda: {
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

    queues.sort(key=lambda x: -(x["jobs"] + x["size"]))

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

    cursor = collection.find(query, fields=fields)

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

  job_data = collection.find_one({"_id": ObjectId(job_id)}, fields=["result"])
  if not job_data:
    return jsonify({})

  return jsonify({
    "result": job_data.get("result")
  })


@app.route('/api/job/<job_id>/traceback')
@requires_auth
def api_job_traceback(job_id):
  collection = connections.mongodb_jobs.mrq_jobs
  job_data = collection.find_one({"_id": ObjectId(job_id)}, fields=["traceback"])

  if not job_data:
    job_data = {}

  return jsonify({
    "traceback": job_data.get("traceback", "No exception raised")
  })


@app.route('/api/jobaction', methods=["POST"])
@requires_auth
def api_job_action():
  return jsonify({
    "job_id": send_task("mrq.basetasks.utils.JobAction", {k: v for k, v in request.form.iteritems()}, queue=get_current_config()["dashboard_queue"])
  })


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

  logs = list(collection.find(query, fields={"_id": 1, "logs": 1}))

  data = {
    "logs": "\n".join([lines["logs"] for lines in logs]),

    # Don't be surprised, this will send unexisting ObjectIds when we're up to date!
    "last_log_id": logs[-1]["_id"] if len(logs) else ObjectId()
  }

  return jsonify(data)


def main():
  app.debug = True
  http = WSGIServer(('', int(os.environ.get("PORT", 5555))), app)
  run_with_reloader(http.serve_forever)


if __name__ == '__main__':
  main()
