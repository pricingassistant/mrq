from future.builtins import range
from .utils import memoize
from . import context


def redis_key(name, *args):
  prefix = context.get_current_config()["redis_prefix"]
  if name == "known_subqueues":
    return "%s:ksq:%s" % (prefix, args[0].root_id)
  elif name == "queue":
     return "%s:q:%s" % (prefix, args[0].id)
  elif name == "started_jobs":
    return "%s:s:started" % prefix
  elif name == "paused_queues":
    return "%s:s:paused" % prefix
  elif name == "notify":
     return "%s:notify:%s" % (prefix, args[0].root_id)


@memoize
def redis_zaddbyscore():
    """ Increments multiple keys in a sorted set & returns them """

    return context.connections.redis.register_script("""
local zset = KEYS[1]
local min = ARGV[1]
local max = ARGV[2]
local offset = ARGV[3]
local count = ARGV[4]
local score = ARGV[5]

local data = redis.call('zrangebyscore', zset, min, max, 'LIMIT', offset, count)
for i, member in pairs(data) do
  redis.call('zadd', zset, score, member)
end

return data
  """)


@memoize
def redis_zpopbyscore():
    """ Pops multiple keys by score """

    return context.connections.redis.register_script("""
local zset = KEYS[1]
local min = ARGV[1]
local max = ARGV[2]
local offset = ARGV[3]
local count = ARGV[4]

local data = redis.call('zrangebyscore', zset, min, max, 'LIMIT', offset, count)
if #data > 0 then
  redis.call('zremrangebyrank', zset, 0, #data - 1)
end

return data
  """)


@memoize
def redis_lpopsafe():
    """ Safe version of LPOP that also adds the key in a "started" zset """

    return context.connections.redis.register_script("""
local key = KEYS[1]
local zset_started = KEYS[2]
local count = ARGV[1]
local now = ARGV[2]
local left = ARGV[3]
local data = {}
local current = nil

for i=1, count do
  if left == '1' then
    current = redis.call('lpop', key)
  else
    current = redis.call('rpop', key)
  end
  if current == false then
    return data
  end
  data[i] = current
  redis.call('zadd', zset_started, now, current)
end

return data
""")


def redis_group_command(command, cnt, redis_key):
    with context.connections.redis.pipeline(transaction=False) as pipe:
        for _ in range(cnt):
            getattr(pipe, command)(redis_key)
        return [x for x in pipe.execute() if x]
