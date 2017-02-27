#!/usr/bin/env python
import os

# Needed to make getaddrinfo() work in pymongo on Mac OS X
# Docs mention it's a better choice for Linux as well.
# This must be done asap in the worker
if "GEVENT_RESOLVER" not in os.environ:
    os.environ["GEVENT_RESOLVER"] = "ares"

from gevent import monkey
monkey.patch_all(subprocess=False)

import sys
import argparse

from .config import get_config
from .agent import Agent
from .context import set_current_config


def main():

    parser = argparse.ArgumentParser(description='Start a MRQ agent')

    cfg = get_config(parser=parser, config_type="agent", sources=("file", "env", "args"))

    set_current_config(cfg)

    agent = Agent()

    agent.work()

    sys.exit(agent.exitcode)


if __name__ == "__main__":
    main()
