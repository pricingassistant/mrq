

def add_standard_worker_arguments(parser):
  parser.add_argument('--maxtasks', default=0, type=int, action='store', help='Gevent: max number of tasks to do before quitting. Temp workaround for memory leaks')
  parser.add_argument('--mongodebug', action='store_true', default=False, help='Print all Mongo requests')
  parser.add_argument('--objgraph', action='store_true', default=False, help='Start objgraph to debug memory after each task')
  parser.add_argument('--profile', action='store_true', default=False, help='Run profiling')
  parser.add_argument('--name', default=None, help='Specify a different name')
  parser.add_argument('queues', nargs='*', help='The queues to listen on (default: \'default\')')
