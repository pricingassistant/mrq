from mrq.agent import Agent
from mrq.context import connections
import time


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
    assert scenario([
        {
            "command": "mrq-worker a",
            "memory": 1000,
            "cpu": 1024,
            "min_count": 1
        }
    ], [
        {
            "_id": "worker1",
            "total_cpu": 1024,
            "total_memory": 1000
        }
    ]) == {
        "worker1": [
            "mrq-worker a"
        ]
    }

    # Not enough memory
    assert scenario([
        {
            "command": "mrq-worker a",
            "memory": 1001,
            "cpu": 1024,
            "min_count": 1
        }
    ], [
        {
            "_id": "worker1",
            "total_cpu": 1024,
            "total_memory": 1000
        }
    ]) == {
        "worker1": []
    }

    # Not enough CPU
    assert scenario([
        {
            "command": "mrq-worker a",
            "memory": 1000,
            "cpu": 1025,
            "min_count": 1
        }
    ], [
        {
            "_id": "worker1",
            "total_cpu": 1024,
            "total_memory": 1000
        }
    ]) == {
        "worker1": []
    }

    # Remove & add workers
    assert scenario([
        {
            "command": "mrq-worker a",
            "memory": 1,
            "cpu": 1,
            "min_count": 2
        },
        {
            "command": "mrq-worker b",
            "memory": 1,
            "cpu": 1,
            "min_count": 1
        }
    ], [
        {
            "_id": "worker1",
            "total_cpu": 3,
            "total_memory": 3,
            "desired_workers": ["mrq-worker c", "mrq-worker b", "mrq-worker b"]
        }
    ]) == {
        "worker1": ["mrq-worker a", "mrq-worker a", "mrq-worker b"]
    }

    # Worker removal & add priority
    assert scenario([
        {
            "command": "mrq-worker a",
            "memory": 1,
            "cpu": 1,
            "min_count": 3
        },
        {
            "command": "mrq-worker b",
            "memory": 1,
            "cpu": 1,
            "min_count": 1
        }
    ], [
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
        "worker1": ["mrq-worker a", "mrq-worker a", "mrq-worker b"],
        "worker2": ["mrq-worker a"]
    }

    worker.stop()


def test_agent_process(worker):

    worker.start(agent=True, flags="--worker_group xxx --total_memory=500 --total_cpu=500 --orchestrate_interval=1 --report_interval=1")

    time.sleep(3)

    agents = list(connections.mongodb_jobs.mrq_agents.find())

    assert len(agents) == 1

    assert connections.mongodb_jobs.mrq_workers.count() == 0

    connections.mongodb_jobs.mrq_workergroups.insert_one({"_id": "xxx", "profiles": [
        {
            "command": "mrq-worker a --report_interval=1",
            "memory": 100,
            "cpu": 100,
            "min_count": 1
        }
    ]})

    time.sleep(7)

    assert connections.mongodb_jobs.mrq_workers.count() == 1
    w = connections.mongodb_jobs.mrq_workers.find_one()
    assert w["status"] in ("spawn", "wait")

    connections.mongodb_jobs.mrq_workergroups.update_one({"_id": "xxx"}, {"$set": {"profiles": [

    ]}})

    time.sleep(4)

    w = connections.mongodb_jobs.mrq_workers.find_one()
    assert w["status"] == "stop"

    assert connections.mongodb_jobs.mrq_agents.count({"status": {"$ne": "stop"}}) == 1

    worker.stop(deps=False)

    time.sleep(2)

    assert connections.mongodb_jobs.mrq_agents.count({"status": {"$ne": "stop"}}) == 0

    worker.stop_deps()
