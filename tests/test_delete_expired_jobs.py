from mrq.job import Job
from mrq.queue import Queue
import datetime

@pytest.mark.skip(reason="Fix local test")
@hiro.Timeline()
def test_delete_expired_jobs(worker, timeline):
    worker.start(flags="--greenlets 1")

    job_id1 = worker.send_task(
        "tests.tasks.general.MongoInsert", {"a": 41, "sleep": 2}, block=False)
    worker.wait_for_idle()
    time.sleep(1)
    worker.stop(deps=False)

    job1 = Job(job_id1).fetch().data

    assert job1["status"] == "success"
    # check if object job1 has dateexpires
    # assert job1["dateexpires"] < datetime.datetime.utcnow()
    assert worker.mongodb_jobs.tests_inserts.count() == 1
    worker.send_task("mrq.basetasks.cleaning.DeleteExpiresJobs", {}, block=True)
    assert worker.mongodb_jobs.tests_inserts.count() == 1
    #Increment time datetime.datetime.utcnow() > assert job1["dateexpires"]
    timeline.forward(7*24*60*60)
    worker.send_task("mrq.basetasks.cleaning.DeleteExpiresJobs", {}, block=True)
    time.sleep(1)
    assert worker.mongodb_jobs.tests_inserts.count() == 0
