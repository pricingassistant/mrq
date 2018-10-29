from mrq.processes import ProcessPool
import time


def test_processpool_sleeps():

    pool = ProcessPool(watch_interval=1)

    pool.start()

    pool.set_commands(["/bin/sleep 1", "/bin/sleep 10"])

    time.sleep(2)

    pool.watch_processes()
    pids1 = {p["command"]: p["pid"] for p in pool.processes}

    time.sleep(2)

    pool.watch_processes()
    assert len(pool.processes) == 2

    pids2 = {p["command"]: p["pid"] for p in pool.processes}

    assert pids1["/bin/sleep 1"] != pids2["/bin/sleep 1"]
    assert pids1["/bin/sleep 10"] == pids2["/bin/sleep 10"]

    pool.set_commands(["/bin/sleep 2", "/bin/sleep 10"])

    time.sleep(3)

    pool.watch_processes()
    assert len(pool.processes) == 2

    pids3 = {p["command"]: p["pid"] for p in pool.processes}
    assert pids3["/bin/sleep 2"] != pids2["/bin/sleep 1"]
    assert pids1["/bin/sleep 10"] == pids3["/bin/sleep 10"]
