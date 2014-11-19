from collections import defaultdict

# Read from tests.tasks.general.GetMetrics
TEST_GLOBAL_METRICS = defaultdict(int)


def METRIC_HOOK(name, incr=1, **kwargs):
    TEST_GLOBAL_METRICS[name] += incr
