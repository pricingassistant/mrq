#!/usr/bin/env python
from gevent import monkey
monkey.patch_all()

import sys
import os

sys.path.append(os.getcwd())

from mrq import worker, config


def main():

  w = worker.Worker(config.get_config())

  w.work_loop()

if __name__ == "__main__":
  main()
