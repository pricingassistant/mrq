from __future__ import print_function
from future.builtins import object
from future.utils import iteritems

from collections import defaultdict
import logging
import datetime
import sys
import pymongo
PY3 = sys.version_info > (3,)


def _encode_if_unicode(string):

    if PY3:
        return string

    if isinstance(string, unicode):  # pylint: disable=undefined-variable
        return string.encode("utf-8", "replace")
    else:
        return string


def _decode_if_str(string):

    if PY3:
        return str(string)

    if isinstance(string, str):
        return string.decode("utf-8", "replace")
    else:
        return unicode(string)  # pylint: disable=undefined-variable


class MongoHandler(logging.Handler):

    """ Job/Worker-aware log handler.

        We used the standard logging module before but it suffers from memory leaks
        when creating lots of logger objects.
    """

    def __init__(self, worker=None, mongodb_logs_size=16 * 1024 * 1024):
        super(MongoHandler, self).__init__()

        self.buffer = {}
        self.collection = None
        self.mongodb_logs_size = mongodb_logs_size

        self.reset()
        self.set_collection()
        # Import here to avoid import loop
        # pylint: disable=cyclic-import
        from .context import get_current_job, get_current_worker
        self.get_current_job = get_current_job
        self.worker = worker

    def set_collection(self):
        from .context import get_current_config, connections
        config = get_current_config()
        collection = config["mongodb_logs"]

        if collection == "1":
            self.collection = connections.mongodb_logs.mrq_logs
        if self.collection and self.mongodb_logs_size:
            if "mrq_logs" in connections.mongodb_logs.collection_names() and not self.collection.options().get("capped"):
                connections.mongodb_logs.command({"convertToCapped": "mrq_logs", "size": self.mongodb_logs_size})
            elif "mrq_logs" not in connections.mongodb_logs.collection_names():
                try:
                    connections.mongodb_logs.create_collection("mrq_logs", capped=True, size=self.mongodb_logs_size)
                except pymongo.errors.OperationFailure:  # The collection might have been created in the meantime
                    pass

    def reset(self):
        self.buffer = {
            "workers": defaultdict(list),
            "jobs": defaultdict(list)
        }

    def emit(self, record):
        log_entry = self.format(record)
        if self.collection is False:
            return
        log_entry = _decode_if_str(log_entry)

        if self.worker is not None:
            self.buffer["workers"][self.worker].append(log_entry)

        if record.name == "mrq.current":
            job_object = self.get_current_job()
            if job_object:
                self.buffer["jobs"][job_object.id].append(log_entry)

    def flush(self):
        # We may log some stuff before we are even connected to Mongo!
        if not self.collection:
            return

        inserts = [{
            "worker": k,
            "logs": "\n".join(v) + "\n"
        } for k, v in iteritems(self.buffer["workers"])] + [{
            "job": k,
            "logs": "\n".join(v) + "\n"
        } for k, v in iteritems(self.buffer["jobs"])]

        if len(inserts) == 0:
            return
        self.reset()

        try:
            self.collection.insert(inserts)
        except Exception as e:  # pylint: disable=broad-except
            self.emit("Log insert failed: %s" % e)
