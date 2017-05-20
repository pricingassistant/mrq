from future import standard_library
standard_library.install_aliases()
from builtins import range
from mrq.task import Task
from mrq.context import (log, retry_current_job, connections, get_current_config, get_current_job,
                         subpool_map, abort_current_job, set_current_job_progress)
from mrq.job import queue_job
import urllib.request, urllib.error, urllib.parse
import json
import time
import copy
from mrq.utils import MongoJSONEncoder


class Add(Task):

    def run(self, params):
        log.info("adding", params)
        res = params.get("a", 0) + params.get("b", 0)

        if params.get("sleep", 0):
            log.info("sleeping", params.get("sleep", 0))
            time.sleep(params.get("sleep", 0))

        return res


class TimeoutFromConfig(Add):
    pass


class GetTime(Task):

    def run(self, params):

        return time.time()


class Fetch(Task):

    def run(self, params):

        f = urllib.request.urlopen(params.get("url"))
        t = f.read()
        f.close()

        return len(t)


class GetConfig(Task):

    def run(self, params):
        return json.dumps(get_current_config())


LEAKS = []


class Leak(Task):

    def run(self, params):

        if params.get("size", 0) > 0:
            #LEAKS.append("".join([random.choice(string.letters) for _ in range(params.get("size", 0))]))
            LEAKS.append(["1" for _ in range(params.get("size", 0))])

        if params.get("sleep", 0) > 0:
            time.sleep(params.get("sleep", 0))

        return params.get("return")


class Retry(Task):

    def run(self, params):

        log.info("Retrying in %s on %s" %
                 (params.get("delay"), params.get("queue")))

        connections.mongodb_jobs.tests_inserts.insert(params)

        retry_current_job(
            queue=params.get("queue"),
            delay=params.get("delay"),
            max_retries=params.get("max_retries")
        )

        raise Exception("Should not be reached")


class InRetryException(BaseException):
    pass


class RetryOnFailed(Task):

    def run(self, params):

        log.info("Retrying in %s on %s" %
                 (params.get("delay"), params.get("queue")))

        connections.mongodb_jobs.tests_inserts.insert(params)
        try:
            raise InRetryException
        except InRetryException:
            retry_current_job(
                queue=params.get("queue"),
                delay=params.get("delay"),
                max_retries=params.get("max_retries")
            )

        raise Exception("Should not be reached")


class WaitForFlag(Task):

    def run(self, params):

        flag = None

        while True:
            flag = connections.mongodb_logs.tests_flags.find_one({"flag": params["flag"]})

            if flag:
                break
            time.sleep(0.1)

        return flag


class RetrySimple(Task):

    def run(self, params):
        connections.mongodb_jobs.tests_inserts.insert(params)

        retry_current_job()

        raise Exception("Should not be reached")


class RaiseException(Task):

    def run(self, params):

        time.sleep(params.get("sleep", 0))

        raise Exception(params.get("message", ""))


class InAbortException(BaseException):
    pass


class Abort(Task):

    def run(self, params):

        abort_current_job()


class AbortOnFailed(Task):
    def run(self, params):

        log.info("Will abort this task")

        connections.mongodb_jobs.tests_inserts.insert(params)
        try:
            raise InAbortException
        except InAbortException:
            abort_current_job()

        raise Exception("Should not be reached")


class ReturnParams(Task):

    def run(self, params):

        time.sleep(params.get("sleep", 0))

        return params


class LargeResult(Task):

    def run(self, params):

        self.status_success_update_w = params["status_success_update_w"]
        self.status_success_update_j = params["status_success_update_j"]

        time.sleep(params.get("sleep", 0))

        return "x" * int(params.get("size", 10))


class Progress(Task):

    def run(self, params):

        for i in range(1, 100):
            set_current_job_progress(0.01 * i, save=params["save"])
            time.sleep(0.1)


class MongoInsert(Task):

    def run(self, params):

        connections.mongodb_jobs.tests_inserts.insert(
            {"params": params}, manipulate=False)

        if params.get("sleep", 0) > 0:
            time.sleep(params.get("sleep", 0))

        return params


MongoInsert2 = MongoInsert


class SubPool(Task):

    def inner(self, x):

        if self.job:
            assert get_current_job() == self.job

        if x == "import-large-file":
            from tests.tasks.largefile import a
            assert a == 1
            return True

        if x == "exception":
            raise Exception(x)

        time.sleep(x)

        return x

    def run(self, params):
        self.job = get_current_job()

        return subpool_map(params["pool_size"], self.inner, params["inner_params"])


class GetMetrics(Task):

    def run(self, params):
        return json.dumps(get_current_config().get("test_global_metrics"))


class GetIoHookEvents(Task):
    def run(self, params):
        events = get_current_config().get("io_events")

        evts = []

        # Remove non-serializable stuff
        for evt in events:
            evt.pop("client", None)
            evt.pop("result", None)
            if evt.get("job"):
                evt["job"] = evt["job"].id
            if evt["hook"].startswith("redis_"):
                evt["key"] = evt["args"][0] if len(evt["args"]) else None
                evt["args"] = repr(evt["args"])
                evt.pop("options", None)

            # print evt
            evts.append(evt)

        return json.dumps(evts, cls=MongoJSONEncoder)


class SendTask(Task):

    def run(self, params):
        return queue_job(params["path"], params["params"])
