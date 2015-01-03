import time
import pytest


@pytest.mark.parametrize(["p_flags"], [
    ["--greenlets 50"],
    ["--processes 10 --greenlets 5"]
])
def test_parallel_100sleeps(worker, p_flags):

    worker.start(flags=p_flags)

    start_time = time.time()

    # This will sleep a total of 100 seconds
    result = worker.send_tasks(
        "tests.tasks.general.Add", [{"a": i, "b": 0, "sleep": 1} for i in range(100)])

    total_time = time.time() - start_time

    # But should be done quickly!
    assert total_time < 15

    # ... and return correct results
    assert result == range(100)
