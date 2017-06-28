#!/usr/bin/env python
import os
import sys
is_pypy = '__pypy__' in sys.builtin_module_names

# Needed to make getaddrinfo() work in pymongo on Mac OS X
# Docs mention it's a better choice for Linux as well.
# This must be done asap in the worker
if "GEVENT_RESOLVER" not in os.environ and not is_pypy:
    os.environ["GEVENT_RESOLVER"] = "ares"

from gevent import monkey
monkey.patch_all()

import argparse

sys.path.insert(0, os.getcwd())

from mrq import config
from mrq.agent import Agent
from mrq.context import set_current_config


def main():

    parser = argparse.ArgumentParser(description='Start a MRQ agent')

    cfg = config.get_config(parser=parser, config_type="agent", sources=("file", "env", "args"))

    set_current_config(cfg)

    agent = Agent()

    agent.work()

    sys.exit(agent.exitcode)


if __name__ == "__main__":
    main()
