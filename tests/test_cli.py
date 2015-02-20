from mrq.job import Job


def test_cli_run_blocking(worker):

    worker.start_deps()

    result = worker.send_task_cli("tests.tasks.general.Add", {"a": 41, "b": 1}, queue=False)

    assert result == 42

    worker.stop_deps()


def test_cli_run_nonblocking(worker):

    worker.start()

    job_id1 = worker.send_task_cli(
        "tests.tasks.general.Add", {"a": 41, "b": 1}, queue="default")

    job1 = Job(job_id1).fetch()

    job1.wait(poll_interval=0.01)

    job1.fetch()

    assert job1.data["status"] == "success"
    assert job1.data["result"] == 42

    worker.stop()
