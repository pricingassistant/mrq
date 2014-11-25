from .context import get_current_job
import time
import random


def patch_pymongo(config):
    """ Monkey-patch pymongo's collections to add some logging """

    # Nothing to change!
    if not config["print_mongodb"] and not config["trace_io"]:
        return

    # Print because we are very early and log() may not be ready yet.
    print "Monkey-patching MongoDB methods..."

    from termcolor import cprint

    def gen_monkey_patch(base_object, method):
        base_method = getattr(base_object, method)

        def mrq_monkey_patched(self, *args, **kwargs):
            if config["print_mongodb"]:
                if self.full_name in config.get(
                        "print_mongodb_hidden_collections",
                        []):
                    cprint("[MONGO] %s.%s%s %s" % (
                        self.full_name, method, "-hidden-", kwargs), "magenta")
                else:
                    cprint("[MONGO] %s.%s%s %s" %
                           (self.full_name, method, args, kwargs), "magenta")

            if config["trace_io"]:
                job = get_current_job()
                if job:
                    job.set_current_io({
                        "type": "mongodb.%s" % method,
                        "data": {
                            "collection": self.full_name
                        }
                        #"data": json.dumps(args)[0:300]  # Perf issue? All MongoDB data will get jsonified!
                    })

            ret = base_method(self, *args, **kwargs)

            if config["trace_io"]:
                job = get_current_job()
                if job:
                    job.set_current_io(None)

            return ret

        return mrq_monkey_patched

    from pymongo.collection import Collection
    for method in ["find", "update", "insert", "remove", "find_and_modify"]:
        if getattr(Collection, method).__name__ != "mrq_monkey_patched":
            setattr(Collection, method, gen_monkey_patch(Collection, method))

    # MongoKit completely replaces the code from PyMongo's find() function, so we
    # need to monkey-patch that as well.
    try:
        from mongokit.collection import Collection as MongoKitCollection
        for method in ["find"]:
            if getattr(
                    MongoKitCollection,
                    method).__name__ != "mrq_monkey_patched":
                setattr(MongoKitCollection, method, gen_monkey_patch(
                    MongoKitCollection, method))

    except ImportError:
        pass


# https://code.google.com/p/gevent/issues/detail?id=108
def patch_import():

    import types
    import gevent.coros
    import __builtin__

    orig_import = __builtin__.__import__
    import_lock = gevent.coros.RLock()

    def mrq_safe_import(*args, **kwargs):
        """
        Normally python protects imports against concurrency by doing some locking
        at the C level (at least, it does that in CPython).  This function just
        wraps the normal __import__ functionality in a recursive lock, ensuring that
        we're protected against greenlet import concurrency as well.
        """
        if len(args) > 0 and type(args[0]) not in [
                types.StringType,
                types.UnicodeType]:
            # if a builtin has been acquired as a bound instance method,
            # python knows not to pass 'self' when the method is called.
            # No such protection exists for monkey-patched builtins,
            # however, so this is necessary.
            args = args[1:]
        import_lock.acquire()
        try:
            result = orig_import(*args, **kwargs)
        finally:
            import_lock.release()
        return result

    builtins = __import__('__builtin__')
    if builtins.__import__.__name__ != "mrq_safe_import":
        builtins.__import__ = mrq_safe_import


def patch_network_latency(seconds=0.01):
    """ Add random latency to all I/O operations """

    from socket import socket as _socket

    # Accept float(0.1), "0.1", "0.1-0.2"
    def sleep():
        if isinstance(seconds, float):
            time.sleep(seconds)
        elif isinstance(seconds, basestring):
            if "-" in seconds:
                time.sleep(random.uniform(
                    float(seconds.split("-")[0]),
                    float(seconds.split("-")[1])
                ))
            else:
                time.sleep(float(seconds))

    class mrq_latency_socket(_socket):
        def send(self, *args, **kwargs):
            sleep()
            return _socket.send(self, *args, **kwargs)

        def sendall(self, *args, **kwargs):
            sleep()
            return _socket.sendall(self, *args, **kwargs)

        def sendto(self, *args, **kwargs):
            sleep()
            return _socket.sendto(self, *args, **kwargs)

        def recv(self, *args, **kwargs):
            sleep()
            return _socket.recv(self, *args, **kwargs)

        def recvfrom(self, *args, **kwargs):
            sleep()
            return _socket.recvfrom(self, *args, **kwargs)

        def recvfrom_into(self, *args, **kwargs):
            sleep()
            return _socket.recvfrom_into(self, *args, **kwargs)

        def recv_into(self, *args, **kwargs):
            sleep()
            return _socket.recv_into(self, *args, **kwargs)

        def connect(self, *args, **kwargs):
            sleep()
            return _socket.connect(self, *args, **kwargs)

    import socket as socketmodule
    if socketmodule.socket.__name__ != "mrq_latency_socket":
        socketmodule.socket = mrq_latency_socket

    import gevent.socket as geventmodule
    if geventmodule.socket.__name__ != "mrq_latency_socket":
        geventmodule.socket = mrq_latency_socket


def patch_io_all():
    """ Patch higher-level modules to provide insights on what is performing IO for each job. """
    patch_io_urllib2()
    patch_io_redis()
    patch_io_pymongo_cursor()

    #patch_io_dns()
    #patch_io_subprocess()


def patch_io_redis():

    from redis import StrictRedis as _StrictRedis

    class mrq_StrictRedis(_StrictRedis):

        def execute_command(self, *args, **options):

            job = get_current_job()
            if job:
                job.set_current_io({
                    "type": "redis.%s" % args[0].lower(),
                    "data": {
                        "key": args[1] if len(args) > 1 else None
                    }
                })

            ret = _StrictRedis.execute_command(self, *args, **options)

            if job:
                job.set_current_io(None)

            return ret

    import redis as redismodule
    if redismodule.StrictRedis.__name__ != "mrq_StrictRedis":
        redismodule.StrictRedis = mrq_StrictRedis


def patch_io_urllib2():

    from urllib import addbase
    from urllib2 import urlopen as _urlopen

    def mrq_urlopen(url, data=None, *args, **kwargs):

        job = get_current_job()

        if not job:
            return _urlopen(url, data, *args, **kwargs)

        def start():
            job.set_current_io({
                "type": "http.get" if data is None else "http.post",
                "data": {
                    "url": url
                }
            })

        def stop():
            job.set_current_io(None)

        class mrq_patched_socket(addbase):
            def __init__(self, *args, **kwargs):
                addbase.__init__(self, *args, **kwargs)
                self.read = self._read

            def _read(self, *args, **kwargs):
                start()
                data = self.fp.read(*args, **kwargs)
                stop()
                return data

        start()
        fp = _urlopen(url, data, *args, **kwargs)
        stop()

        return mrq_patched_socket(fp)

    import urllib2 as urllib2module
    if urllib2module.urlopen.__name__ != "mrq_urlopen":
        urllib2module.urlopen = mrq_urlopen


def patch_io_pymongo_cursor():

    from pymongo.cursor import Cursor

    class mrq_patched_pymongo_cursor(Cursor):

        # Some dark magic is needed here to cope with python's name mangling for private variables.
        def _Cursor__send_message(self, *args, **kwargs):
            # print self.__dict__
            job = get_current_job()

            if job:

                subtype = "find"
                collection = self._Cursor__collection.name  # pylint: disable-msg=E1101

                if collection == "$cmd":
                    items = self._Cursor__spec.items()  # pylint: disable-msg=E1101
                    if len(items) > 0:
                        subtype, collection = items[0]

                job.set_current_io({
                    "type": "mongodb.%s" % subtype,
                    "data": {
                        "collection": "%s.%s" % (self._Cursor__collection.database.name, collection)  # pylint: disable-msg=E1101
                    }
                })
            ret = Cursor._Cursor__send_message(self, *args, **kwargs)  # pylint: disable-msg=E1101

            if job:
                job.set_current_io(None)
            return ret

    import pymongo as pymongomodule
    if pymongomodule.cursor.Cursor.__name__ != "mrq_patched_pymongo_cursor":
        pymongomodule.cursor.Cursor = mrq_patched_pymongo_cursor

    if pymongomodule.collection.Cursor.__name__ != "mrq_patched_pymongo_cursor":
        pymongomodule.collection.Cursor = mrq_patched_pymongo_cursor
