##
## Redis Test
##

HOST = "127.0.0.1"
PORT = 6379

assert("Redis#select") do
  r = Redis.new HOST, PORT
  r.select 0
  r.set "score", "0"
  db0_score =  r.get("score")

  r.select 1
  r.set "score", "1"
  db1_score = r.get("score")

  r.select 0
  db0_1_score = r.get("score")

  r.close

  assert_equal "0", db0_score
  assert_equal "1", db1_score
  assert_equal "0", db0_1_score
end

assert("Redis#set, Redis#get") do
  r = Redis.new HOST, PORT
  r.set "hoge", "fuga"
  ret = r.get "hoge"
  r.close

  assert_equal "fuga", ret
end

assert("Redis#[]=, Redis#[]") do
  r = Redis.new HOST, PORT
  r["hoge"] = "fuga"
  r["ho\0ge"] = "fu\0ga"
  ret1 = r["hoge"]
  ret2 = r["ho\0ge"]
  r.close

  assert_equal "fuga", ret1
  assert_equal "fu\0ga", ret2
end

assert("Redis#exist?") do
  r = Redis.new HOST, PORT
  r.del "hoge"
  r.del "fuga"
  r.del "pi\0yo"
  r["hoge"] = "aaa"
  r["pi\0yo"] = "bbb"
  ret1 = r.exists? "hoge"
  ret2 = r.exists? "fuga"
  ret3 = r.exists? "hoge\0"
  ret4 = r.exists? "piyo"
  ret5 = r.exists? "pi\0yo"
  r.close

  assert_true ret1
  assert_false ret2
  assert_false ret3
  assert_false ret4
  assert_true ret5
end

assert("Redis#expire") do
  r = Redis.new HOST, PORT
  r.del "hoge"

  expire = r.expire "hoge", 1
  r.set "hoge", "a"
  expire2 = r.expire "hoge", 1
  ret = r.get "hoge"

  # 1000_000 micro sec is sensitive time 
  # so 1100_000 micro sec is sufficient time.
  usleep 1100_000
  
  ret2 = r.get "hoge"

  r.close

  assert_false expire
  assert_true expire2
  assert_equal "a", ret
  assert_nil ret2
end

assert("Redis#del") do
  r = Redis.new HOST, PORT
  r.set "hoge", "a"
  ret = r.exists? "hoge"
  r.del "hoge"
  ret2 = r.exists? "hoge"

  r.set "fu\0ga", "b"
  ret3 = r.exists? "fu\0ga"
  r.del "fu\0ga"
  ret4 = r.exists? "fu\0ga"
  r.close

  assert_true ret
  assert_false ret2
  assert_true ret3
  assert_false ret4
end

assert("Redis#hset", "Redis#hget") do
  r = Redis.new HOST, PORT
  r.del "myhash"
  r.del "myhash\0"

  ret_set_f1_a = r.hset "myhash", "field1", "a"
  ret_get_f1_a = r.hget "myhash", "field1"
  ret_set_f1_b = r.hset "myhash", "field1", "b"
  ret_get_f1_b = r.hget "myhash", "field1"

  r.hset "myhash", "field2", "c"
  ret_get_f2 = r.hget "myhash", "field2"

  ret_set_f3_d = r.hset "myhash\0", "field3\0", "d\0"
  ret_get_f3_d = r.hget "myhash\0", "field3\0"
  ret_set_f3_e = r.hset "myhash\0", "field3\0", "e\0"
  ret_get_f3_e = r.hget "myhash\0", "field3\0"

  r.hset "myhash\0", "field4\0", "f\0"
  ret_get_f4 = r.hget "myhash\0", "field4\0"
  r.close

  assert_true ret_set_f1_a    # true if field is a new field in the hash and value was set.
  assert_false ret_set_f1_b   # false if field already exists in the hash and the value was updated.
  assert_true ret_set_f3_d
  assert_false ret_set_f3_e

  assert_equal "a", ret_get_f1_a
  assert_equal "b", ret_get_f1_b

  assert_equal "c", ret_get_f2

  assert_equal "d\0", ret_get_f3_d
  assert_equal "e\0", ret_get_f3_e

  assert_equal "f\0", ret_get_f4
end

assert("Redis#hgetall") do
  r = Redis.new HOST, PORT
  r.del "myhash"

  hash = r.hgetall "myhash"

  r.hset "myhash", "field1", "a"
  r.hset "myhash", "field2", "b"
  r.hset "myhash", "field3\0", "c"
  r.hset "myhash", "field4", "d\0"

  hash2 = r.hgetall "myhash"

  r.close

  assert_nil hash
  assert_equal({"field1"=>"a", "field2"=>"b","field3\0"=>"c", "field4"=>"d\0"}, hash2)
end

assert("Redis#hdel") do
  r = Redis.new HOST, PORT
  r.del "myhash"
  r.del "myhash\0"

  r.hset "myhash", "field1", "a"
  r.hset "myhash", "field2", "b"
  r.hset "myhash\0", "field3\0", "c"
  r.hset "myhash\0", "field4\0", "d"
  ret_get_f1_a = r.hget "myhash", "field1"
  ret_get_f3_c = r.hget "myhash\0", "field3\0"
  r.hdel "myhash", "field1"
  r.hdel "myhash\0", "field3\0"
  ret_get_f1_nil = r.hget "myhash", "field1"
  ret_get_f3_nil = r.hget "myhash", "field3\0"
  ret_exists = r.exists?("myhash")
  ret_exists2 = r.exists?("myhash\0")

  r.close

  assert_equal "a", ret_get_f1_a
  assert_equal "c", ret_get_f3_c
  assert_nil ret_get_f1_nil
  assert_nil ret_get_f3_nil
  assert_true ret_exists
  assert_true ret_exists2
end

assert("Redis#hkeys") do
  r = Redis.new HOST, PORT
  r.del "myhash"

  keys =  r.hkeys "myhash"

  r.hset "myhash", "field1", "a"
  r.hset "myhash", "field2", "b"
  r.hset "myhash", "field3\0", "c"
  r.hset "myhash", "field4", "d\0"

  keys2 =  r.hkeys "myhash"

  r.close

  assert_nil keys
  assert_equal ["field1", "field2", "field3\0", "field4"], keys2
end

assert("Redis#incrby") do
  r = Redis.new HOST, PORT
  r.del "score"
  r.del "score\0"

  r.set "score", "10"
  r.set "score\0", "20"
  ret = r.incrby "score", 100
  ret2 = r.incrby "score\0", 200
  score = r.get "score"
  score2 = r.get "score\0"

  r.close

  assert_equal 110, ret
  assert_equal 220, ret2
  assert_equal "110", score
  assert_equal "220", score2
end

assert("Redis#decrby") do
  r = Redis.new HOST, PORT
  r.del "score"
  r.del "score\0"

  r.set "score", "10"
  r.set "score\0", "20"
  ret = r.decrby "score", 100
  ret2 = r.decrby "score\0", 200
  score = r.get "score"
  score2 = r.get "score\0"

  r.close

  assert_equal(-90, ret)
  assert_equal(-180, ret2)
  assert_equal "-90", score
  assert_equal "-180", score2
end

assert("Redis#lpush", "Redis#lrange") do
  r = Redis.new HOST, PORT
  r.del "logs"

  range1 = r.lrange "logs", 0, -1
  ret1 = r.lpush "logs", "error1"
  ret2 = r.lpush "logs", "error2\0"
  ret3 = r.lpush "logs", "error3"
  range2 = r.lrange "logs", 0, 0
  range3 = r.lrange "logs", -3, 2
  range4 = r.lrange "logs", 0, -1
  range5 = r.lrange "logs", -100, 100
  range6 = r.lrange "logs", 5, 10

  r.close

  assert_equal 1, ret1
  assert_equal 2, ret2
  assert_equal 3, ret3

  assert_equal [], range1
  assert_equal ["error3"], range2
  assert_equal ["error3", "error2\0", "error1"], range3
  assert_equal ["error3", "error2\0", "error1"], range4
  assert_equal ["error3", "error2\0", "error1"], range5
  assert_equal [], range6
end

assert("Redis#rpop") do
  r = Redis.new HOST, PORT
  r.del "list"

  r.rpush "list", "one"
  r.rpush "list", "two"
  r.rpush "list", "three\0"
  range1 = r.lrange "list", 0, -1
  ret = r.rpop "list"
  range2 = r.lrange "list", 0, -1

  r.close

  assert_equal ["one", "two", "three\0"], range1
  assert_equal "three\0", ret
  assert_equal ["one", "two"], range2
end

assert("Redis#lpop") do
  r = Redis.new HOST, PORT
  r.del "list"

  r.rpush "list", "one\0"
  r.rpush "list", "two"
  r.rpush "list", "three"
  range1 = r.lrange "list", 0, -1
  ret = r.lpop "list"
  range2 = r.lrange "list", 0, -1

  r.close

  assert_equal ["one\0", "two", "three"], range1
  assert_equal "one\0", ret
  assert_equal ["two", "three"], range2
end

assert("Redis#ttl") do
  r = Redis.new HOST, PORT
  r.del "hoge"
  r.del "hoge\0"
  r.del "fuga"
  r.del "fuga\0"

  r.set "hoge", "a"  
  r.set "hoge\0", "a"  
  r.expire "hoge", 1
  r.expire "hoge\0", 1
  ttl = r.ttl "hoge"
  ttl4 = r.ttl "hoge\0"

  # 1_000_000 micro sec is sensitive time 
  # so 1_100_000 micro sec is sufficient time.
  usleep 1_100_000

  ttl2 = r.ttl "hoge"
  ttl5 = r.ttl "hoge\0"
  r.set "fuga", "b"
  ttl3 = r.ttl "fuga"

  r.close

  assert_equal 1, ttl
  assert_equal -2, ttl2
  assert_equal -1, ttl3
  assert_equal 1, ttl4
  assert_equal -2, ttl5
end

# got erro for travis ci. comment out until fix the problems
#assert("Redis#zadd, Redis#zrange") do
#  r = Redis.new HOST, PORT
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
#  r = Redis.new HOST, PORT
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
#  r = Redis.new HOST, PORT
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
#  r = Redis.new HOST, PORT
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
# - randomkey
# - ltrim
# - publish
