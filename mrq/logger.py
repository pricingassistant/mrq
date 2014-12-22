
from collections import defaultdict
import datetime


def _encode_if_unicode(string):
    if isinstance(string, unicode):
        return string.encode("utf-8", "replace")
    else:
        return string


def _decode_if_str(string):
    if isinstance(string, str):
        return string.decode("utf-8", "replace")
    else:
        return unicode(string)


class LogHandler(object):

    """ Job/Worker-aware log handler.

        We used the standard logging module before but it suffers from memory leaks
        when creating lots of logger objects.
    """

    def __init__(self, collection=None, quiet=False):

        self.buffer = {}
        self.collection = None

        self.reset()
        self.set_collection(collection)
        self.quiet = quiet

        # Import here to avoid import loop
        # pylint: disable=cyclic-import
        from .context import get_current_job
        self.get_current_job = get_current_job

    def get_logger(self, worker=None, job=None):
        return Logger(self, worker=worker, job=job)

    def set_collection(self, collection=None):
        self.collection = collection

    def reset(self):
        self.buffer = {
            "workers": defaultdict(list),
            "jobs": defaultdict(list)
        }

    def log(self, level, *args, **kwargs):

        worker = kwargs.get("worker")
        job = kwargs.get("job")

        joined_unicode_args = u" ".join([_decode_if_str(x) for x in args])
        formatted = u"%s [%s] %s" % (
            datetime.datetime.utcnow(), level.upper(), joined_unicode_args)

        if not self.quiet:
            try:
                print _encode_if_unicode(formatted)
            except UnicodeDecodeError:
                print formatted

        if self.collection is False:
            return

        if worker is not None:
            self.buffer["workers"][worker].append(formatted)
        elif job is not None:
            if job == "current":
                job_object = self.get_current_job()
                if job_object:
                    self.buffer["jobs"][job_object.id].append(formatted)
            else:
                self.buffer["jobs"][job].append(formatted)

    def flush(self, w=0):

        # We may log some stuff before we are even connected to Mongo!
        if not self.collection:
            return

        inserts = [{
            "worker": k,
            "logs": "\n".join(v) + "\n"
        } for k, v in self.buffer["workers"].iteritems()] + [{
            "job": k,
            "logs": "\n".join(v) + "\n"
        } for k, v in self.buffer["jobs"].iteritems()]

        if len(inserts) == 0:
            return

        self.reset()

        try:
            self.collection.insert(inserts, w=w)
        except Exception as e:  # pylint: disable=broad-except
            from mrq.context import get_current_worker
            self.log("debug", "Log insert failed: %s" % e, worker=get_current_worker())


class Logger(object):

    """ This object acts as a logger from python's logging module. """

    def __init__(self, handler, **kwargs):
        self._handler = handler
        self.kwargs = kwargs
        self.quiet = False

    @property
    def handler(self):
        if self._handler:
            return self._handler

        # Import here to avoid import loop
        from .context import get_current_worker
        worker = get_current_worker()

        if worker:
            return worker.log_handler
        else:
            self._handler = LogHandler(quiet=self.quiet)
            return self._handler

    def info(self, *args):
        self.handler.log("info", *args, **self.kwargs)

    def warning(self, *args):
        self.handler.log("warning", *args, **self.kwargs)

    def error(self, *args):
        self.handler.log("error", *args, **self.kwargs)

    def debug(self, *args):
        self.handler.log("debug", *args, **self.kwargs)
