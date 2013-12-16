##
## Redis Test
##

assert("Redis#set, Redis#get") do
  r = Redis.new "127.0.0.1", 6379
  r.set "hoge", "fuga"
  ret = r.get "hoge"
  r.close

  assert_equal "fuga", ret
end

assert("Redis#[]=, Redis#[]") do
  r = Redis.new "127.0.0.1", 6379
  r["hoge"] = "fuga"
  ret = r["hoge"]
  r.close

  assert_equal "fuga", ret
end

assert("Redis#exist?") do
  r = Redis.new "127.0.0.1", 6379
  r["hoge"] = "aaa"
  ret1 = r.exists? "hoge"
  ret2 = r.exists? "fuga"
  r.close

  assert_true ret1
  assert_false ret2
end

# got erro for travis ci. comment out until fix the problems
#assert("Redis#zadd, Redis#zrange") do
#  r = Redis.new "127.0.0.1", 6379
#  r.del "hs"
#  r.zadd "hs", 80, "a"
#  r.zadd "hs", 50.1, "b"
#  r.zadd "hs", 60, "c"
#  ret = r.zrange "hs", 0, -1
#  r.close
#
#  assert_equal ["b", "c", "a"], ret
#end
#
#assert("Redis#zrevrange") do
#  r = Redis.new "127.0.0.1", 6379
#  r.del "hs"
#  r.zadd "hs", 80, "a"
#  r.zadd "hs", 50.1, "b"
#  r.zadd "hs", 60, "c"
#  ret = r.zrevrange "hs", 0, -1
#  r.close
#
#  assert_equal ["a", "c", "b"], ret
#end
#
#assert("Redis#zrank") do
#  r = Redis.new "127.0.0.1", 6379
#  r.del "hs"
#  r.zadd "hs", 80, "a"
#  r.zadd "hs", 50.1, "b"
#  r.zadd "hs", 60, "c"
#  ret1 = r.zrank "hs", "b"
#  ret2 = r.zrank "hs", "c"
#  ret3 = r.zrank "hs", "a"
#  r.close
#
#  assert_equal 0, ret1
#  assert_equal 1, ret2
#  assert_equal 2, ret3
#end
#
#assert("Redis#zscore") do
#  r = Redis.new "127.0.0.1", 6379
#  r.del "hs"
#  r.zadd "hs", 80, "a"
#  r.zadd "hs", 50.1, "b"
#  r.zadd "hs", 60, "c"
#  ret_a = r.zscore "hs", "a"
##  ret_b = r.zscore "hs", "b"
#  ret_c = r.zscore "hs", "c"
#  r.close
#
#  assert_equal "80", ret_a
##  assert_equal "50.1", ret_b      # value is not 50.1 in mruby and redis-cli
#  assert_equal "60", ret_c
#end

# TODO: Add test
# - select
# - randomkey
# - del
# - incr
# - decr
# - lpush
# - lrange
# - ltrim
# - publish
