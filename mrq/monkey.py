from .context import get_current_job


def patch_pymongo(config):
  """ Monkey-patch pymongo's collections to add some logging """

  # Nothing to change!
  if not config["print_mongodb"] and not config["trace_mongodb"]:
    return

  # Print because we are very early and log() may not be ready yet.
  print "Monkey-patching MongoDB methods..."

  from termcolor import cprint

  def gen_monkey_patch(base_object, method):
    base_method = getattr(base_object, method)

    def mrq_monkey_patched(self, *args, **kwargs):
      if config["print_mongodb"]:
        if self.full_name in config.get("print_mongodb_hidden_collections", []):
          cprint("[MONGO] %s.%s%s %s" % (self.full_name, method, "-hidden-", kwargs), "magenta")
        else:
          cprint("[MONGO] %s.%s%s %s" % (self.full_name, method, args, kwargs), "magenta")

      if config["trace_mongodb"]:
        job = get_current_job()
        if job:
          job._trace_mongodb[method] += 1
      return base_method(self, *args, **kwargs)

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
      if getattr(MongoKitCollection, method).__name__ != "mrq_monkey_patched":
        setattr(MongoKitCollection, method, gen_monkey_patch(MongoKitCollection, method))

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
