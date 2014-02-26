import json
import datetime
from bson.objectid import ObjectId
from werkzeug import Response


class MongoJsonEncoder(json.JSONEncoder):
  def default(self, obj):  # pylint: disable-msg=E0202
    if isinstance(obj, (datetime.datetime, datetime.date)):
      return obj.isoformat()
    elif isinstance(obj, ObjectId):
      return unicode(obj)
    return json.JSONEncoder.default(self, obj)


def jsonify(*args, **kwargs):
  """ jsonify with support for MongoDB ObjectId
  """
  return Response(json.dumps(dict(*args, **kwargs), cls=MongoJsonEncoder), mimetype='application/json')
