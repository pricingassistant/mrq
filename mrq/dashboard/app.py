from gevent import monkey
monkey.patch_all()

from flask import Flask, request
from utils import jsonify
import os
import sys
from bson import ObjectId

sys.path.append(os.getcwd())

from mrq.worker import Worker
from mrq.config import get_config
from mrq.queue import send_task

app = Flask("dashboard", static_folder='mrq/dashboard/static')

worker = None


@app.route('/')
def root():
  return app.send_static_file("index.html")


@app.route('/api/datatables/<unit>')
def api_datatables(unit):


  collection = None

  if unit == "queues":
    # TODO MongoDB distinct?
    queues = [{"name": k} for k in worker.redis.keys()]
    for q in queues:
      q["count"] = worker.redis.llen(q["name"])

    data = {
      "aaData": queues,
      "iTotalDisplayRecords": len(queues)
    }

  if unit == "workers":
    fields = None
    query = {}
    collection = worker.mongodb_logs.mrq_workers
  elif unit == "jobs":
    fields = None
    query = {}
    for param in ["queue", "worker", "path", "status"]:
      if request.args.get(param):
        query[param] = request.args.get(param)
    if request.args.get("id"):
      query["_id"] = ObjectId(request.args.get("id"))

    # We can't search easily params because we store it as decoded JSON in mongo :(
    # Add a string index?
    # if request.args.get("sSearch"):
    #   query.update(json.loads(request.args.get("sSearch")))
    collection = worker.mongodb_logs.mrq_jobs

  if collection is not None:
    data = {
      "aaData": list(collection.find(query, fields=fields).skip(int(request.args.get("iDisplayStart", 0))).limit(int(request.args.get("iDisplayLength", 20)))),
      "iTotalDisplayRecords": collection.find(query).count()
    }

  data["sEcho"] = request.args["sEcho"]

  return jsonify(data)


@app.route('/api/job/<job_id>/result')
def api_job_result(job_id):
  collection = worker.mongodb_jobs.mrq_jobs

  job_data = collection.find_one({"_id": ObjectId(job_id)}, fields=["result"])
  if not job_data:
    return jsonify({})

  return jsonify({
    "result": job_data.get("result")
  })


@app.route('/api/jobaction', methods=["POST"])
def api_job_action():
  return jsonify({
    "job_id": send_task("mrq.basetasks.utils.JobAction", {k: v for k, v in request.form.iteritems()})
  })


@app.route('/api/logs')
def api_logs():
  collection = worker.mongodb_logs.mrq_logs

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
    "last_log_id": logs[-1]["_id"] if len(logs) else ObjectId()
  }

  return jsonify(data)

if __name__ == '__main__':

  worker = Worker(get_config(sources=("args", "env")))
  worker.connect()

  app.debug = True
  app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5555)))
