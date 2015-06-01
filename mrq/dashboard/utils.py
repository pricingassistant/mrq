import json
# from werkzeug import Response
from functools import wraps
from flask import request, Response
from mrq.context import get_current_config
from mrq.utils import MongoJSONEncoder


def jsonify(*args, **kwargs):
    """ jsonify with support for MongoDB ObjectId
    """
    return Response(
        json.dumps(
            dict(
                *args,
                **kwargs),
            cls=MongoJSONEncoder),
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
