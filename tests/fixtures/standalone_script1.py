from mrq.context import setup_context, run_task, get_current_config

# Autoconfigure MRQ's environment
setup_context()

print run_task("tests.tasks.general.Add", {"a": 41, "b": 1})

print get_current_config()["name"]