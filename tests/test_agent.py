from mrq.agent import Agent
from mrq.context import connections
import time
from mrq.job import Job, get_job_result
import psutil


def scenario(profiles, agents):
    connections.mongodb_jobs.mrq_agents.delete_many({})
    connections.mongodb_jobs.mrq_workergroups.delete_many({})

    for agent in agents:
        agent["worker_group"] = "xx"

    connections.mongodb_jobs.mrq_agents.insert_many(agents + [{"worker_group": "zz"}])
    connections.mongodb_jobs.mrq_workergroups.insert_one({"_id": "xx", "profiles": profiles})

    agent = Agent(worker_group="xx")
    agent.orchestrate()

    return {a["_id"]: sorted(a.get("desired_workers", [])) for a in connections.mongodb_jobs.mrq_agents.find({"worker_group": "xx"})}


def test_orchestration_scenarios(worker):

    # Only for the deps...
    worker.start()

    # Simplest scenario
    assert scenario({
        "a": {
            "command": "mrq-worker a",
            "memory": 1000,
            "cpu": 1024,
            "min_count": 1
        }
    }, [
        {
            "_id": "worker1",
            "total_cpu": 1024,
            "total_memory": 1000
        }
    ]) == {
        "worker1": [
            "MRQ_WORKER_PROFILE=a mrq-worker a"
        ]
    }

    # Not enough memory
    assert scenario({
        "a": {
            "command": "mrq-worker a",
            "memory": 1001,
            "cpu": 1024,
            "min_count": 1
        }
    }, [
        {
            "_id": "worker1",
            "total_cpu": 1024,
            "total_memory": 1000
        }
    ]) == {
        "worker1": []
    }

    # Not enough CPU
    assert scenario({
        "a": {
            "command": "mrq-worker a",
            "memory": 1000,
            "cpu": 1025,
            "min_count": 1
        }
    }, [
        {
            "_id": "worker1",
            "total_cpu": 1024,
            "total_memory": 1000
        }
    ]) == {
        "worker1": []
    }

    # Remove & add workers
    assert scenario({
        "a": {
            "command": "mrq-worker a",
            "memory": 1,
            "cpu": 1,
            "min_count": 2
        },
        "b": {
            "command": "mrq-worker b",
            "memory": 1,
            "cpu": 1,
            "min_count": 1
        }
    }, [
        {
            "_id": "worker1",
            "total_cpu": 3,
            "total_memory": 3,
            "desired_workers": ["mrq-worker c", "mrq-worker b", "mrq-worker b"]
        }
    ]) == {
        "worker1": [
            "MRQ_WORKER_PROFILE=a mrq-worker a",
            "MRQ_WORKER_PROFILE=a mrq-worker a",
            "MRQ_WORKER_PROFILE=b mrq-worker b"
        ]
    }

    # Worker removal & add priority
    assert scenario({
        "a": {
            "command": "mrq-worker a",
            "memory": 1,
            "cpu": 1,
            "min_count": 3
        },
        "b": {
            "command": "mrq-worker b",
            "memory": 1,
            "cpu": 1,
            "min_count": 1
        }
    }, [
        {
            "_id": "worker1",
            "total_cpu": 11,
            "total_memory": 11,
            "desired_workers": ["mrq-worker a", "mrq-worker a"]
        }, {
            "_id": "worker2",
            "total_cpu": 5,
            "total_memory": 5,
            "desired_workers": ["mrq-worker a", "mrq-worker a"]
        }
    ]) == {
        "worker1": [
            "MRQ_WORKER_PROFILE=a mrq-worker a",
            "MRQ_WORKER_PROFILE=a mrq-worker a",
            "MRQ_WORKER_PROFILE=b mrq-worker b"
        ],
        "worker2": ["MRQ_WORKER_PROFILE=a mrq-worker a"]
    }

    # Worker diversity enforced under constraints
    assert scenario({
        "a": {
            "command": "mrq-worker a",
            "memory": 1,
            "cpu": 1,
            "min_count": 3
        },
        "b": {
            "command": "mrq-worker b",
            "memory": 1,
            "cpu": 1,
            "min_count": 1
        }
    }, [
        {
            "_id": "worker1",
            "total_cpu": 2,
            "total_memory": 2,
            "desired_workers": ["mrq-worker a", "mrq-worker a"]
        }
    ]) == {
        "worker1": [
            "MRQ_WORKER_PROFILE=a mrq-worker a",
            "MRQ_WORKER_PROFILE=b mrq-worker b"
        ]
    }


def test_agent_process(worker):

    worker.start(agent=True, flags="--worker_group xxx --total_memory=500 --total_cpu=500 --orchestrate_interval=1 --report_interval=1")

    time.sleep(3)

    agents = list(connections.mongodb_jobs.mrq_agents.find())

    assert len(agents) == 1

    assert connections.mongodb_jobs.mrq_workers.count() == 0

    connections.mongodb_jobs.mrq_workergroups.insert_one({"_id": "xxx", "profiles": {
        "a": {
            "command": "mrq-worker a --report_interval=1",
            "memory": 100,
            "cpu": 100,
            "min_count": 1
        }
    }})

    time.sleep(7)

    assert connections.mongodb_jobs.mrq_workers.count() == 1
    w = connections.mongodb_jobs.mrq_workers.find_one()
    assert w["status"] in ("spawn", "wait")

    connections.mongodb_jobs.mrq_workergroups.update_one({"_id": "xxx"}, {"$set": {"profiles": {}}})

    time.sleep(4)

    w = connections.mongodb_jobs.mrq_workers.find_one()
    assert w["status"] == "stop"

    assert connections.mongodb_jobs.mrq_agents.count({"status": {"$ne": "stop"}}) == 1

    worker.stop(deps=False)

    time.sleep(2)

    assert connections.mongodb_jobs.mrq_agents.count({"status": {"$ne": "stop"}}) == 0

    worker.stop_deps()


def test_agent_autoscaling(worker):

    worker.start(agent=True, flags="--worker_group xxx --total_memory=500 --total_cpu=500 --orchestrate_interval=1 --report_interval=1  --autoscaling_taskpath tests.tasks.agent.Autoscale")

    connections.mongodb_jobs.mrq_workergroups.insert_one({"_id": "xxx", "profiles": {
        "a": {
            "command": "mrq-worker default --report_interval=1 --greenlets 2",
            "memory": 100,
            "cpu": 100,
            "min_count": 1,
            "max_count": 3,
            "max_eta": 10,
            "warmup": 5
        }
    }})

    time.sleep(5)

    assert connections.mongodb_jobs.mrq_workers.count({"status": {"$in": ["wait", "spawn"]}}) == 1
    assert connections.mongodb_jobs.mrq_workers.count() == 1

    # Inserted by the autoscaling task
    assert connections.mongodb_jobs.tests_inserts.count() > 0

    # Send 2 tasks with sleep(1) each second. That should not trigger an autoscale
    for i in range(10):
        worker.send_tasks(
            "tests.tasks.general.Add", [{"a": 41, "b": i, "sleep": 1} for _ in range(2)], block=False)
        time.sleep(1)

    assert connections.mongodb_jobs.mrq_workers.count() == 1

    # Now send 4 of them
    for i in range(10):
        worker.send_tasks(
            "tests.tasks.general.Add", [{"a": 41, "b": i, "sleep": 1} for _ in range(4)], block=False)
        time.sleep(1)

    # Should have scaled to 2
    assert connections.mongodb_jobs.mrq_workers.count() == 2
    assert connections.mongodb_jobs.mrq_workers.count({"status": {"$in": ["wait", "spawn", "full"]}}) == 2

    # Now send 10 of them - this should be too much but we should obey the max of 3 workers.
    for i in range(10):
        worker.send_tasks(
            "tests.tasks.general.Add", [{"a": 41, "b": i, "sleep": 1} for _ in range(10)], block=False)
        time.sleep(1)

    assert connections.mongodb_jobs.mrq_workers.count() == 3
    assert connections.mongodb_jobs.mrq_workers.count({"status": {"$in": ["wait", "spawn", "full"]}}) == 3

    # Kill all jobs
    assert connections.mongodb_jobs.mrq_jobs.update_many({}, {"$set": {"status": "cancel"}})

    time.sleep(40)

    # We should be back to 1 or 2
    assert connections.mongodb_jobs.mrq_workers.count({"status": {"$in": ["wait", "spawn", "full"]}}) < 3


def test_agent_force_terminate(worker):

    worker.start(agent=True, flags="--worker_group xxx --total_memory=500 --total_cpu=500 --orchestrate_interval=1 --report_interval=1")

    #
    # First, test interrupting a worker doing only a sleeping process.
    #

    connections.mongodb_jobs.mrq_workergroups.insert_one({"_id": "xxx", "profiles": {
        "a": {
            "command": "mrq-worker default --report_interval=60",
            "memory": 100,
            "cpu": 100,
            "min_count": 1
        }
    }, "process_termination_timeout": 1})

    time.sleep(5)

    job1 = worker.send_task("tests.tasks.general.Add", {"a": 41, "b": 2, "sleep": 100}, block=False)

    time.sleep(3)

    res1 = get_job_result(job1)
    assert res1["status"] == "started"

    connections.mongodb_jobs.mrq_workergroups.update_one({"_id": "xxx"}, {"$set": {"profiles": {
        "a": {
            "command": "mrq-worker otherqueue --report_interval=60",
            "memory": 100,
            "cpu": 100,
            "min_count": 1
        }
    }, "process_termination_timeout": 1}})

    time.sleep(5)

    res1 = get_job_result(job1)
    assert res1["status"] == "interrupt"

    #
    # Then, test interrupting an infinite-looping task
    #

    job2 = worker.send_task("tests.tasks.general.CPULoop", {}, block=False, queue="otherqueue")

    time.sleep(3)

    res2 = get_job_result(job2)
    assert res2["status"] == "started"

    pids_before_sigkill = psutil.pids()

    connections.mongodb_jobs.mrq_workergroups.update_one({"_id": "xxx"}, {"$set": {"profiles": {
        "a": {
            "command": "mrq-worker otherqueue2 --report_interval=60",
            "memory": 100,
            "cpu": 100,
            "min_count": 1
        }
    }, "process_termination_timeout": 1}})

    # SIGKILL is sent after 5 seconds
    time.sleep(10)

    # Because we are SIGKILLing, the job won't even be marked as interrupt!
    res2 = get_job_result(job2)
    assert res2["status"] == "started"

    # However, we make sure there is no leftover process and that the worker was replaced.
    pids_after_sigkill = psutil.pids()

    assert len(pids_after_sigkill) == len(pids_before_sigkill)
    assert set(pids_after_sigkill) != set(pids_before_sigkill)
