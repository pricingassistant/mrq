from mrq.job import Job
import time


def test_kill_by_id(worker):
    worker.start()

    job_id1 = worker.send_task("tests.tasks.general.Sleep", {}, block=False)

    Job(job_id1).kill()
    time.sleep(1)

    job1 = Job(job_id1).fetch().data

    assert job1["status"] == "killed"


# def test_kill_by_path(worker):
#     worker.start()

#     job_id1 = worker.send_task("tests.tasks.general.Sleep", {}, block=False)
#     job_id2 = worker.send_task("tests.tasks.general.Sleep", {}, block=False)

#     worker.send_task("mrq.basetasks.utils.JobAction", {
#         "path": "tests.tasks.general.Sleep",
#         "action": "kill"
#     }, block=False)

#     time.sleep(1)

#     job1 = Job(job_id1).fetch().data
#     job2 = Job(job_id2).fetch().data

#     assert job1["status"] == "killed"
#     assert job2["status"] == "killed"
