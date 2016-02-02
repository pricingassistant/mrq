
# Read from tests.tasks.general.GetIoHookEvents
IO_EVENTS = []


def MONGODB_PRE_HOOK(event):
    event["hook"] = "mongodb_pre"
    IO_EVENTS.append(event)


def MONGODB_POST_HOOK(event):
    event["hook"] = "mongodb_post"
    IO_EVENTS.append(event)


def REDIS_PRE_HOOK(event):
    event["hook"] = "redis_pre"
    IO_EVENTS.append(event)


def REDIS_POST_HOOK(event):
    event["hook"] = "redis_post"
    IO_EVENTS.append(event)
