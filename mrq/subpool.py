from itertools import count as itertools_count
import traceback
import time
import gevent


def subpool_map(pool_size, func, iterable):
    """ Starts a Gevent pool and run a map. Takes care of setting current_job and cleaning up. """

    from .context import get_current_job, set_current_job, log

    if not pool_size:
        return [func(*args) for args in iterable]

    counter = itertools_count()

    current_job = get_current_job()

    def inner_func(*args):
        """ As each call to 'func' will be done in a random greenlet of the subpool, we need to
            register their IDs with set_current_job() to make get_current_job() calls work properly
            inside 'func'.
        """
        next(counter)
        if current_job:
            set_current_job(current_job)

        try:
          ret = func(*args)
        except Exception as exc:
          trace = traceback.format_exc()
          exc.subpool_traceback = trace
          raise

        if current_job:
            set_current_job(None)
        return ret

    def inner_iterable():
        """ This will be called inside the pool's main greenlet, which ID also needs to be registered """
        if current_job:
            set_current_job(current_job)

        for x in iterable:
            yield x

        if current_job:
            set_current_job(None)

    start_time = time.time()
    pool = gevent.pool.Pool(size=pool_size)
    ret = pool.map(inner_func, inner_iterable())
    pool.join(raise_error=True)
    total_time = time.time() - start_time

    log.debug("SubPool ran %s greenlets in %0.6fs" % (counter, total_time))

    return ret


def subpool_imap(pool_size, func, iterable, flatten=False, unordered=False, buffer_size=None):
  """ Generator version of subpool_map. Should be used with unordered=True for optimal performance """

  from .context import get_current_job, set_current_job, log

  if not pool_size:
    for args in iterable:
      yield func(*args)

  counter = itertools_count()

  current_job = get_current_job()

  def inner_func(*args):
    """ As each call to 'func' will be done in a random greenlet of the subpool, we need to
        register their IDs with set_current_job() to make get_current_job() calls work properly
        inside 'func'.
    """
    next(counter)
    if current_job:
      set_current_job(current_job)

    try:
      ret = func(*args)
    except Exception as exc:
      trace = traceback.format_exc()
      exc.subpool_traceback = trace
      raise

    if current_job:
      set_current_job(None)
    return ret

  def inner_iterable():
    """ This will be called inside the pool's main greenlet, which ID also needs to be registered """
    if current_job:
      set_current_job(current_job)

    for x in iterable:
      yield x

    if current_job:
      set_current_job(None)

  start_time = time.time()
  pool = gevent.pool.Pool(size=pool_size)

  if unordered:
    iterator = pool.imap_unordered(inner_func, inner_iterable(), maxsize=buffer_size or pool_size)
  else:
    iterator = pool.imap(inner_func, inner_iterable())

  for x in iterator:
    if flatten:
      for y in x:
        yield y
    else:
      yield x

  pool.join(raise_error=True)
  total_time = time.time() - start_time

  log.debug("SubPool ran %s greenlets in %0.6fs" % (counter, total_time))
