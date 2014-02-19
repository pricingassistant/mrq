from gevent import monkey
monkey.patch_all()

from flask import Flask, request
from utils import jsonify
import os
import sys

sys.path.append(os.getcwd())

from mrq.worker import Worker
from mrq.config import get_config

app = Flask("dashboard", static_folder='mrq/dashboard/static')

worker = None


@app.route('/')
def root():
  return app.send_static_file("index.html")


@app.route('/api/datatables/<unit>')
def api_datatables(unit):

  worker.connect()

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
    for param in ["queue", "worker", "path"]:
      if request.args.get(param):
        query[param] = request.args.get(param)

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


if __name__ == '__main__':

  worker = Worker(get_config(sources=("args", "env")))

  app.debug = True
  app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5555)))
