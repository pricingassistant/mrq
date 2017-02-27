from mrq.agent import Agent
from mrq.context import connections


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
            "available_cpu": 1024,
            "available_memory": 1000
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
            "available_cpu": 1024,
            "available_memory": 1000
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
            "available_cpu": 1024,
            "available_memory": 1000
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
            "available_cpu": 3,
            "available_memory": 3,
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
            "available_cpu": 11,
            "available_memory": 11,
            "desired_workers": ["mrq-worker a", "mrq-worker a"]
        }, {
            "_id": "worker2",
            "available_cpu": 5,
            "available_memory": 5,
            "desired_workers": ["mrq-worker a", "mrq-worker a"]
        }
    ]) == {
        "worker1": ["mrq-worker a", "mrq-worker a", "mrq-worker b"],
        "worker2": ["mrq-worker a"]
    }

    worker.stop()
