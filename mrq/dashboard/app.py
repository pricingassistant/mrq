from gevent import monkey
monkey.patch_all()

from flask import Flask, request

import os
import sys
from bson import ObjectId

sys.path.insert(0, os.getcwd())

from mrq.queue import send_task, Queue
from mrq.context import connections, set_current_config, get_current_config
from mrq.config import get_config

from utils import jsonify, requires_auth

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

set_current_config(get_config())
app = Flask("dashboard", static_folder=os.path.join(CURRENT_DIRECTORY, "static"))


@app.route('/')
@requires_auth
def root():
  return app.send_static_file("index.html")


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


@app.route('/api/datatables/<unit>')
@requires_auth
def api_datatables(unit):

  collection = None
  sort = None

  if unit == "queues":
    # TODO MongoDB distinct?
    queues = [{
      "name": queue.id,
      "count": queue.size()
    } for queue in Queue.all()]

    queues.sort(key=lambda x: -x["count"])

    data = {
      "aaData": queues,
      "iTotalDisplayRecords": len(queues)
    }

  if unit == "workers":
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
    query = {}
    sort = [("_id", 1)]

    if request.args.get("redisqueue"):
      query["_id"] = {"$in": [ObjectId(x) for x in Queue(request.args.get("redisqueue")).list_job_ids(limit=1000)]}
    else:

      for param in ["queue", "path", "status"]:
        if request.args.get(param):
          query[param] = request.args.get(param)
      if request.args.get("id"):
        query["_id"] = ObjectId(request.args.get("id"))
      if request.args.get("worker"):
        query["worker"] = ObjectId(request.args.get("worker"))

    # We can't search easily params because we store it as decoded JSON in mongo :(
    # Add a string index?
    # if request.args.get("sSearch"):
    #   query.update(json.loads(request.args.get("sSearch")))
    collection = connections.mongodb_jobs.mrq_jobs

  if collection is not None:

    cursor = collection.find(query, fields=fields)

    if sort:
      cursor.sort(sort)

    data = {
      "aaData": list(cursor.skip(int(request.args.get("iDisplayStart", 0))).limit(int(request.args.get("iDisplayLength", 20)))),
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
  app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5555)))


if __name__ == '__main__':
  main()
