from future import standard_library
standard_library.install_aliases()
from past.builtins import basestring
from .context import get_current_job, get_current_worker
import time
import random
import re
import copy


def patch_method(base_class, method_name, method):

    # Create a closure to store the old method
    def _patched_factory(old_method):
        def _mrq_patched_method(*args, **kwargs):
            return method(old_method, *args, **kwargs)
        return _mrq_patched_method

    old_method = getattr(base_class, method_name)
    setattr(base_class, method_name, _patched_factory(old_method))


def patch_io_all(config):
    """ Patch higher-level modules to provide insights on what is performing IO for each job. """

    patch_io_httplib(config)
    patch_io_redis(config)
    patch_io_pymongo_cursor(config)

    #patch_io_dns()
    #patch_io_subprocess()


def patch_pymongo(config):
    """ Monkey-patch pymongo's collections to add some logging """

    # Nothing to change!
    if not config["print_mongodb"] and not config["trace_io"]:
        return

    from termcolor import cprint

    # Print because we are very early and log() may not be ready yet.
    cprint("Monkey-patching MongoDB methods...", "white")

    def gen_monkey_patch(base_object, method):
        base_method = getattr(base_object, method)

        def mrq_monkey_patched(self, *args, **kwargs):

            if config["trace_io"]:
                comment = "mrq"

                worker = get_current_worker()
                job = get_current_job()
                if job:
                    job.set_current_io({
                        "type": "mongodb.%s" % method,
                        "data": {
                            "collection": self.full_name
                        }
                        # Perf issue? All MongoDB data will get jsonified!
                        # "data": json.dumps(args)[0:300]
                    })
                    comment = {"job": job.id}
                elif worker:
                    comment = {"worker": worker.id}

                # Tag potentially expensive queries with their job id for easier debugging
                if method in ["find", "find_and_modify", "count", "update_many", "update", "delete_many"]:
                    if len(args) > 0 and isinstance(args[0], dict) and "$comment" not in args[0]:
                        query = copy.copy(args[0])
                        query["$comment"] = comment
                        args = (query, ) + args[1:]

            if config["print_mongodb"]:
                if self.full_name in config.get("print_mongodb_hidden_collections", []):
                    cprint("[MONGO] %s.%s%s %s" % (
                        self.full_name, method, "-hidden-", kwargs
                    ), "magenta")
                else:
                    cprint("[MONGO] %s.%s%s %s" % (
                        self.full_name, method, args, kwargs
                    ), "magenta")

            if config.get("mongodb_pre_hook"):
                config.get("mongodb_pre_hook")({
                    "collection": self.full_name,
                    "method": method,
                    "args": args,
                    "kwargs": kwargs,
                    "client": self.database.client,
                    "job": job
                })

            start_time = time.time()
            ret = False
            try:
                ret = base_method(self, *args, **kwargs)
            finally:
                stop_time = time.time()

                job = None

                if config["trace_io"]:
                    job = get_current_job()
                    if job:
                        job.set_current_io(None)

                if config.get("mongodb_post_hook"):
                    config.get("mongodb_post_hook")({
                        "collection": self.full_name,
                        "method": method,
                        "args": args,
                        "kwargs": kwargs,
                        "client": self.database.client,
                        "job": job,
                        "result": ret,
                        "time": stop_time - start_time
                    })

            return ret

        # Needed to avoid breaking mongokit
        mrq_monkey_patched.__doc__ = method.__doc__

        return mrq_monkey_patched

    from pymongo.collection import Collection
    pymongo_method_whitelist = (
        "bulk_write",
        "find", "find_one_and_delete", "find_one_and_replace", "find_one_and_update",
        "update", "update_one", "update_many",
        "drop",
        "count",
        "save",
        "insert", "insert_one", "insert_many",
        "replace_one",
        "remove", "delete_one", "delete_many",
        "find_and_modify",
        "parallel_scan",
        "options",
        "aggregate",
        "group", "distinct",
        "rename",
        "map_reduce", "inline_map_reduce",
        "create_indexes", "create_index", "ensure_index", "drop_index", "reindex", "list_indexes"
    )
    for method in pymongo_method_whitelist:
        if hasattr(Collection, method) and getattr(Collection, method).__name__ != "mrq_monkey_patched":
            setattr(Collection, method, gen_monkey_patch(Collection, method))


def patch_network_latency(seconds=0.01):
    """ Add random latency to all I/O operations """

    # Accept float(0.1), "0.1", "0.1-0.2"
    def sleep():
        if isinstance(seconds, float):
            time.sleep(seconds)
        elif isinstance(seconds, basestring):
            # pylint: disable=maybe-no-member
            if "-" in seconds:
                time.sleep(random.uniform(
                    float(seconds.split("-")[0]),
                    float(seconds.split("-")[1])
                ))
            else:
                time.sleep(float(seconds))

    def _patched_method(old_method, *args, **kwargs):
        sleep()
        return old_method(*args, **kwargs)

    socket_methods = [
        "send", "sendall", "sendto", "recv", "recvfrom", "recvfrom_into", "recv_into",
        "connect", "connect_ex", "close"
    ]

    from socket import socket as _socketmodule
    from gevent.socket import socket as _geventmodule
    from gevent.ssl import SSLSocket as _sslmodule   # pylint: disable=no-name-in-module

    for method in socket_methods:
        patch_method(_socketmodule, method, _patched_method)
        patch_method(_geventmodule, method, _patched_method)
        patch_method(_sslmodule, method, _patched_method)


def patch_io_redis(config):

    def execute_command(old_method, self, *args, **options):

        job = get_current_job()
        if job:
            job.set_current_io({
                "type": "redis.%s" % args[0].lower(),
                "data": {
                    "key": args[1] if len(args) > 1 else None
                }
            })

        if config.get("redis_pre_hook"):
            config.get("redis_pre_hook")({
                "command": args[0],
                "args": args[1:],
                "options": options,
                "client": self,
                "job": job
            })

        start_time = time.time()
        ret = False
        try:
            ret = old_method(self, *args, **options)
        finally:
            stop_time = time.time()

            if job:
                job.set_current_io(None)

            if config.get("redis_post_hook"):
                config.get("redis_post_hook")({
                    "command": args[0],
                    "args": args[1:],
                    "options": options,
                    "time": stop_time - start_time,
                    "result": ret,
                    "job": job,
                    "client": self
                })

        return ret

    from redis import StrictRedis

    patch_method(StrictRedis, "execute_command", execute_command)


def patch_io_httplib(config):
    """ Patch the base httplib.HTTPConnection class, which is used in most HTTP libraries
        like urllib2 or urllib3/requests. """

    # pylint: disable=import-error

    def start(method, url):
        job = get_current_job()
        if job:
            job.set_current_io({
                "type": "http.%s" % method.lower(),
                "data": {
                    "url": url
                }
            })

    def stop():
        job = get_current_job()
        if job:
            job.set_current_io(None)

    class mrq_wrapped_socket(object):
        """ Socket-like object that keeps track of 'trace_args' and wraps our monitoring code
            around blocking I/O calls. """

        def __init__(self, obj, parent_connection):
            self._obj = obj
            self._parent_connection = parent_connection

            def _make_patched_method(method):
                def _patched_method(*args, **kwargs):

                    # In the case of HTTPS, we may connect() before having called conn.request()
                    # For requests/urllib3, we may need to plug ourselves at the
                    # connectionpool.urlopen level
                    if not hasattr(self._parent_connection, "_traced_args"):
                        return getattr(self._obj, method)(*args, **kwargs)

                    start(*self._parent_connection._traced_args)  # pylint: disable=protected-access
                    try:
                        data = getattr(self._obj, method)(*args, **kwargs)
                    finally:
                        stop()
                    return data
                return _patched_method

            # Replace socket methods with instrumented ones
            for method in [

              # socket
              "send", "sendall", "sendto", "recv", "recvfrom", "recvfrom_into", "recv_into",
              "connect", "connect_ex", "close",

              # fileobject
              "read", "readline", "write", "writelines", "seek"
            ]:
                setattr(self, method, _make_patched_method(method))

        # Forward all other calls/attributes to the base socket
        def __getattr__(self, attr):
            # cprint(attr, "green")
            return getattr(self._obj, attr)

        def makefile(self, *args, **kwargs):
            newsock = self._obj.makefile(*args, **kwargs)
            return mrq_wrapped_socket(newsock, self._parent_connection)

    def request(old_method, self, method, url, body=None, headers=None, *args, **kwargs):

        if headers is None:
            headers = {}

        # This is for proxy support - TODO show that in dashboard?
        if re.search(r"^http(s?)\:\/\/", url):
            report_url = url
        else:
            protocol = "http"
            if hasattr(self, "key_file"):
                protocol = "https"

            report_url = "%s://%s%s%s" % (
                protocol,
                self.host,
                (":%s" % self.port) if self.port != 80 else "",
                url
            )

        self._traced_args = (method, report_url)  # pylint: disable=protected-access
        res = old_method(self, method, url, body=body, headers=headers)
        return res

    def connect(old_method, self, *args, **kwargs):

        # In the case of HTTPS, we may connect() before having called conn.request()
        # For requests/urllib3, we may need to plug ourselves at the connectionpool.urlopen level
        if not hasattr(self, "_traced_args"):
            ret = old_method(self, *args, **kwargs)
        else:
            start(*self._traced_args)  # pylint: disable=protected-access
            try:
                ret = old_method(self, *args, **kwargs)
            finally:
                stop()
        self.sock = mrq_wrapped_socket(self.sock, self)

        return ret

    from http.client import HTTPConnection, HTTPSConnection

    patch_method(HTTPConnection, "request", request)
    patch_method(HTTPConnection, "connect", connect)
    patch_method(HTTPSConnection, "connect", connect)

    # Try to patch requests & urllib3 as they are very popular python modules.
    try:
        from requests.packages.urllib3.connection import (
            HTTPConnection,
            UnverifiedHTTPSConnection,
            VerifiedHTTPSConnection
        )

        patch_method(HTTPConnection, "connect", connect)
        patch_method(UnverifiedHTTPSConnection, "connect", connect)
        patch_method(VerifiedHTTPSConnection, "connect", connect)

    except ImportError:
        pass

    try:
        from urllib3.connection import (
            HTTPConnection,
            UnverifiedHTTPSConnection,
            VerifiedHTTPSConnection
        )

        patch_method(HTTPConnection, "connect", connect)
        patch_method(UnverifiedHTTPSConnection, "connect", connect)
        patch_method(VerifiedHTTPSConnection, "connect", connect)

    except ImportError:
        pass


def patch_io_pymongo_cursor(config):

    from pymongo.cursor import Cursor

    class mrq_patched_pymongo_cursor(Cursor):

        # Some dark magic is needed here to cope with python's name mangling for private variables.
        def _Cursor__send_message(self, *args, **kwargs):

            subtype = "cursor"
            collection = self._Cursor__collection.name  # pylint: disable=no-member

            if collection == "$cmd":
                items = list(self._Cursor__spec.items())  # pylint: disable=no-member
                if len(items) > 0:
                    subtype, collection = items[0]

            full_name = "%s.%s" % (self._Cursor__collection.database.name, collection)  # pylint: disable=no-member

            job = get_current_job()

            if job:

                job.set_current_io({
                    "type": "mongodb.%s" % subtype,
                    "data": {
                        "collection": full_name
                    }
                })

            if config.get("mongodb_pre_hook"):

                config.get("mongodb_pre_hook")({
                    "collection": full_name,
                    "method": subtype,
                    "args": (getattr(args[0], "spec", None), ),
                    "kwargs": kwargs,
                    "client": self._Cursor__collection.database.client,
                    "job": job
                })

            start_time = time.time()
            ret = False
            try:
                ret = Cursor._Cursor__send_message(self, *args, **kwargs)  # pylint: disable=no-member
            finally:
                stop_time = time.time()
                if job:
                    job.set_current_io(None)

                if config.get("mongodb_post_hook"):
                    config.get("mongodb_post_hook")({
                        "collection": full_name,
                        "method": subtype,
                        "args": (getattr(args[0], "spec", None), ),
                        "kwargs": kwargs,
                        "client": self._Cursor__collection.database.client,
                        "job": job,
                        "result": ret,
                        "time": stop_time - start_time
                    })

            return ret

    import pymongo as pymongomodule
    if pymongomodule.cursor.Cursor.__name__ != "mrq_patched_pymongo_cursor":
        pymongomodule.cursor.Cursor = mrq_patched_pymongo_cursor

    if pymongomodule.collection.Cursor.__name__ != "mrq_patched_pymongo_cursor":
        pymongomodule.collection.Cursor = mrq_patched_pymongo_cursor
