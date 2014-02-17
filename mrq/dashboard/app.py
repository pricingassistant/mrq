from gevent import monkey
monkey.patch_all()

from flask import Flask, jsonify
import os
import sys

sys.path.append(os.getcwd())

from mrq.worker import Worker
from mrq.config import get_config

app = Flask("dashboard")


STATIC_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'static')


@app.route('/')
def root():
  return app.send_static_file("index.html")


@app.route('/api/workers.json')
def api_root():
  return jsonify({
    "workers": list(worker.mongodb_logs.mrq_workers.find({}))
  })


if __name__ == '__main__':

  worker = Worker(get_config(sources=("args", "env")))

  app.debug = True
  app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5555)))
