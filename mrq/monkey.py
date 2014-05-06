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

    def monkey_patched(self, *args, **kwargs):
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

    return monkey_patched

  from pymongo.collection import Collection
  for method in ["find", "update", "insert", "remove", "find_and_modify", "aggregate"]:
    setattr(Collection, method, gen_monkey_patch(Collection, method))

  # MongoKit completely replaces the code from PyMongo's find() function, so we
  # need to monkey-patch that as well.
  try:
    from mongokit.collection import Collection as MongoKitCollection
    for method in ["find"]:
      setattr(MongoKitCollection, method, gen_monkey_patch(MongoKitCollection, method))

  except:
    pass
