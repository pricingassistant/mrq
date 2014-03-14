from .context import get_current_config, get_current_job


def patch_pymongo(verbose=False, trace=False):
  """ Monkey-patch pymongo's collections to add some logging """

  # Nothing to change!
  if not verbose and not trace:
    return

  from pymongo.collection import Collection
  from termcolor import cprint

  methods = ["find", "update", "insert", "remove", "find_and_modify", "aggregate"]

  config = get_current_config()

  def gen_monkey_patch(method):
    base_method = getattr(Collection, method)

    def monkey_patched(self, *args, **kwargs):
      if verbose:
        if self.full_name in config.get("print_mongodb_hidden_collections", []):
          cprint("[MONGO] %s.%s%s %s" % (self.full_name, method, "-hidden-", kwargs), "magenta")
        else:
          cprint("[MONGO] %s.%s%s %s" % (self.full_name, method, args, kwargs), "magenta")

      if trace:
        job = get_current_job()
        if job:
          job._trace_mongodb[method] += 1
      return base_method(self, *args, **kwargs)

    return monkey_patched

  for method in methods:
    setattr(Collection, method, gen_monkey_patch(method))
