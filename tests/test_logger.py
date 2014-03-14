from bson import ObjectId
import urllib2
import json
import time
import pytest


@pytest.mark.parametrize(["class_name"], [["string"], ["unicode"], ["latin-1"]])
def test_supports_string_and_unicode(worker, class_name):

  result = worker.send_task("mrq.basetasks.tests.logger.Simple", {"class_name": class_name})
  worker.stop(deps=False)
  assert result

  time.sleep(0.1)

  # Job logs
  db_logs = list(worker.mongodb_logs.mrq_logs.find({"job": {"$exists": True}}))
  assert len(db_logs) == 1
  if class_name == "unicode":
    assert u"caf\xe9" in db_logs[0]["logs"]
  elif class_name == "string":
    assert u"cafe" in db_logs[0]["logs"]
  elif class_name == "latin-1":
    assert "caf" in db_logs[0]["logs"]
    assert u"cafe" not in db_logs[0]["logs"]
    assert u"caf\xe9" not in db_logs[0]["logs"]

  # Worker logs
  # db_logs = list(worker.mongodb_logs.mrq_logs.find({"worker": db_workers[0]["_id"]}))
  # assert len(db_logs) >= 1
  # if class_name == "unicode":
  #   assert u"caf\xe9" in db_logs
  # else:
  #   assert u"cafe" in db_logs
