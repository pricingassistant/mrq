import json
import datetime
from bson.objectid import ObjectId
# from werkzeug import Response
from functools import wraps
from flask import request, Response
from mrq.context import get_current_config


class MongoJsonEncoder(json.JSONEncoder):

    def default(self, obj):  # pylint: disable=E0202
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return unicode(obj)
        return json.JSONEncoder.default(self, obj)


def jsonify(*args, **kwargs):
    """ jsonify with support for MongoDB ObjectId
    """
    return Response(
        json.dumps(
            dict(
                *args,
                **kwargs),
            cls=MongoJsonEncoder),
        mimetype='application/json')


def check_auth(username, pwd):
    """This function is called to check if a username /
    password combination is valid.
    """
    cfg = get_current_config()
    return username == cfg["dashboard_httpauth"].split(
        ":")[0] and pwd == cfg["dashboard_httpauth"].split(":")[1]


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )


def requires_auth(f):

    cfg = get_current_config()
    if not cfg["dashboard_httpauth"]:
        return f

    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated
