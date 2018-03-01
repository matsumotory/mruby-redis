##
## Redis Test
##

HOST         = "127.0.0.1"
PORT         = 6379
SECURED_PORT = 6380

assert("Redis#ping") do
  r = Redis.new HOST, PORT

  assert_equal "PONG", r.ping
end

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

assert("Redis#set, Redis#get for non-string") do
  r = Redis.new HOST, PORT
  assert_raise(TypeError) {r.set :hoge, 'bar'}
  assert_raise(TypeError) {r.set 'hoge', 10}
  assert_raise(TypeError) {r.get :hoge}
  r.close
end

assert("Redis#set, Redis#get") do
  r = Redis.new HOST, PORT
  r.set( "hoge", "fuga", {EX: "10", PX: "1"})
  ret = r.get "hoge"
  r.close

  assert_equal "fuga", ret
end

assert("Redis#set, Redis#get") do
  r = Redis.new HOST, PORT
  r.set( "hoge", "fuga", {EX: "10", PX: "1"})
  ret = r.get "hoge"
  r.close

  assert_equal "fuga", ret
end

assert("Redis#setnx, Redis#get") do
  r = Redis.new HOST, PORT
  r.flushall
  assert_true r.setnx( "hoge", "fuga")
  assert_false r.setnx( "hoge", "piyo")
  ret = r.get "hoge"

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

assert("Redis#flushall") do
  r = Redis.new HOST, PORT
  r.set "key1", "a"
  r.set "key2", "b"
  ret1 = r.exists? "key1"
  ret2 = r.exists? "key2"
  r.flushall
  ret_after1 = r.exists? "key1"
  ret_after2 = r.exists? "key2"
  r.close

  assert_true ret1
  assert_true ret2
  assert_false ret_after1
  assert_false ret_after2
end

assert("Redis#flushdb") do
  r = Redis.new HOST, PORT
  r.set "key1", "a"
  r.set "key2", "b"
  ret1 = r.exists? "key1"
  ret2 = r.exists? "key2"
  r.flushdb
  ret_after1 = r.exists? "key1"
  ret_after2 = r.exists? "key2"
  r.close

  assert_true ret1
  assert_true ret2
  assert_false ret_after1
  assert_false ret_after2
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

assert("Redis#hsetnx") do
  r = Redis.new HOST, PORT
  r.del "myhash"

  ret1 = r.hsetnx "myhash", "field1", "a"
  ret2 = r.hget "myhash", "field1"
  ret3 = r.hsetnx "myhash", "field1", "b"
  ret4 = r.hget "myhash", "field1"

  assert_true ret1
  assert_equal "a", ret2
  assert_false ret3
  assert_equal "a", ret4
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

assert("Redis#hexists?") do
  r = Redis.new HOST, PORT
  r.del "myhash"
  r.del "myhash\0"

  r.hset "myhash", "field", "a"
  r.hset "myhash\0", "field\0", "b\0"
  ret_hexists = r.hexists?("myhash", "field")
  ret_hexists2 = r.hexists?("myhash", "invalid_field")
  ret_hexists3 = r.hexists?("myhash\0", "field\0")
  ret_hexists4 = r.hexists?("myhash\0", "invalid_field")

  r.close

  assert_true ret_hexists
  assert_false ret_hexists2
  assert_true ret_hexists3
  assert_false ret_hexists4
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

assert("Redis#hmset, Redis#hmget") do
  r = Redis.new HOST, PORT
  r.del "myhash"
  r.del "myhash\0"

  r.hmset "myhash", "field1", "a", "field2", "b"
  r.hmset "myhash", "field2", "c"
  ret_get_f12_ab = r.hmget "myhash", "field1", "field2"

  r.hmset "myhash\0", "field3\0", "c\0", "field4\0", "d\0"
  ret_get_f34_cd = r.hmget "myhash\0", "field3\0", "field4\0"

  ret_get_f12n_abn = r.hmget "myhash", "field1", "field2", "nofield"

  assert_raise(ArgumentError) {r.hmget "myhash"}
  assert_raise(ArgumentError) {r.hmset "myhash"}
  assert_raise(ArgumentError) {r.hmset "myhash", "field1"}
  assert_raise(ArgumentError) {r.hmset "myhash", "field1", "a", "field2"}

  r.close

  assert_equal ["a", "c"], ret_get_f12_ab
  assert_equal ["c\0", "d\0"], ret_get_f34_cd
  assert_equal ["a", "c", nil], ret_get_f12n_abn
end

assert("Redis#hvals") do
  r = Redis.new HOST, PORT
  r.del "myhash"

  vals =  r.hvals "myhash"

  r.hset "myhash", "field1", "a"
  r.hset "myhash", "field2", "b"
  r.hset "myhash", "field3\0", "c"
  r.hset "myhash", "field4", "d\0"

  vals2 =  r.hvals "myhash"

  r.close

  assert_nil vals
  assert_equal ["a", "b", "c", "d\0"], vals2
end

assert("Redis#hincrby") do
  r = Redis.new HOST, PORT
  r.del "myhash"

  r.hset "myhash", "score", "10"
  r.hset "myhash", "score\0", "20"
  ret = r.hincrby "myhash", "score", 100
  ret2 = r.hincrby "myhash", "score\0", 200
  score = r.hget "myhash", "score"
  score2 = r.hget "myhash", "score\0"

  r.close

  assert_equal 110, ret
  assert_equal 220, ret2
  assert_equal "110", score
  assert_equal "220", score2
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

assert('Redis#mget') do
  r = Redis.new HOST, PORT

  r.set "foo", "hoge"
  r.set "bar", "fuga"
  ret1 = r.mget("foo", "bar")
  r.flushall

  r.set "one", "1"
  ret2 = r.mget("one", "two")
  r.flushall

  ret3 = r.mget("piyo","puyo")
  r.close

  assert_equal ["hoge", "fuga"], ret1
  assert_equal ["1", nil], ret2
  assert_equal [nil, nil], ret3
end

assert('Redis#mset, Redis#mget') do
  r = Redis.new HOST, PORT

  r.mset "hoge", "one", "fuga", "two"
  ret = r.mget("hoge", "fuga", "piyo")
  r.flushall

  r.close

  assert_equal ["one", "two", nil ], ret
end


assert("Redis#sadd") do
  r = Redis.new HOST, PORT
  assert_equal 1, r.sadd('set', 'test')
  r.spop 'set'

  assert_equal 1, r.sadd('set', 'bar')
  assert_equal 1, r.scard('set')

  assert_equal 1, r.sismember('set', 'bar')
  assert_equal 0, r.sismember('set', 'buzz')
  
  assert_equal 'bar', r.spop('set')

  assert_equal 2, r.sadd('set', 'bar', 'buzz')
  assert_equal 2, r.scard('set')

  assert_equal 1, r.sismember('set', 'bar')
  assert_equal 1, r.sismember('set', 'buzz')
  assert_equal 0, r.sismember('set', 'foo')

  assert_equal ['bar', 'buzz'], r.smembers('set').sort

  r.flushall

end

assert("Redis#sismember Redis#scard Redis#smembers Redis#spop") do
  r = Redis.new HOST, PORT

  assert_equal 0, r.scard('set')

  r.sadd 'set', 'bar'

  assert_equal 1, r.scard('set')

  assert_equal ['bar'], r.smembers('set')
  
  assert_equal 'bar', r.spop('set')

end

assert("Redis#srem") do
  r = Redis.new HOST, PORT

  r.sadd 'set', 'hoge'

  assert_equal 1, r.scard('set')

  assert_equal 1, r.srem("set", "hoge")
  assert_equal 0, r.srem("set", "fuga") 

  assert_equal 0, r.scard('set')

  r.sadd 'set', 'a', 'b', 'c'

  assert_equal 3, r.scard('set')

  assert_equal 2, r.srem('set', 'a', 'b')
  assert_equal 1, r.srem('set', 'c', 'd', 'e')
  assert_equal 0, r.srem('set', 'x', 'y', 'z')

  assert_equal 0, r.scard('set')

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

  assert_equal( 1, ttl)
  assert_equal(-2, ttl2)
  assert_equal(-1, ttl3)
  assert_equal( 1, ttl4)
  assert_equal(-2, ttl5)
end

assert("Redis#keys") do
  r = Redis.new HOST, PORT

  r.set "foo", "foo"
  r.set "bar", "bar"

  only_foo = r.keys 'fo*'
  only_bar = r.keys '*ar'

  r.close

  assert_equal ['foo'], only_foo
  assert_equal ['bar'], only_bar
end

assert("Redis#llen") do
  r = Redis.new HOST, PORT

  r.lpush('mylist', 'hello')
  r.lpush('mylist', 'world')
  len = r.llen('mylist')

  r.close

  assert_equal 2, len
end

assert("Redis#lindex") do
  r = Redis.new HOST, PORT

  r.lpush('mylist', 'hello')
  r.lpush('mylist', 'world')
  val = r.lindex('mylist', 2)

  r.close

  assert_equal 'world', val
end

assert("Redis#new") do
  assert_raise(Redis::ConnectionError) { Redis.new "10.10.10.10", 6379 }
  assert_raise(Redis::ConnectionError) { Redis.new "10.10.10.10", 6379, 0 }
  assert_raise(Redis::ConnectionError) { Redis.new "10.10.10.10", 6379, 1 }
  assert_raise(TypeError)    { Redis.new "10.10.10.10", 6379, nil }
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

assert("Redis#zcard") do
  r = Redis.new HOST, PORT
  r.zadd "myzset", 1, "one"
  r.zadd "myzset", 2, "two"

  assert_equal 2, r.zcard("myzset")
end

assert("Pipelined commands") do
  redis = Redis.new HOST, PORT
  redis.queue(:set, "mruby-redis-test:foo", "bar")
  redis.queue(:get, "mruby-redis-test:foo")
  assert_equal(:OK,   redis.reply)
  assert_equal("bar", redis.reply)

  redis.queue(:set, "mruby-redis-test:foo", "bar")
  redis.queue(:get, "mruby-redis-test:foo")
  assert_equal([:OK, "bar"], redis.bulk_reply)

  redis.queue(:nonexistant)
  assert_kind_of(Redis::ReplyError, redis.reply)
  redis.del("mruby-redis-test:foo")
end

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

assert("Redis#randomkey") do
  r = Redis.new HOST, PORT
  r.flushdb
  r.set "foo", "bar"

  assert_equal "foo", r.randomkey
end

assert("Redis#ltrim") do
  r = Redis.new HOST, PORT
  r.rpush "mylist", "one"
  r.rpush "mylist", "two"
  r.rpush "mylist", "three"

  r.ltrim "mylist", 1, -1

  results = r.lrange "mylist", 0, -1
  assert_equal ["two", "three"], results
end

assert("Redis#publish") do
  producer = Redis.new HOST, PORT

  assert_equal 0, producer.publish("some_queue", "hello world")
end

assert("Redis#pfadd") do
  r = Redis.new HOST, PORT
  assert_equal 1, r.pfadd("foos")
  assert_equal 0, r.pfadd("foos")
  assert_equal 1, r.pfadd("foos", "bar")
  assert_equal 1, r.pfadd("foos", "baz")
  assert_equal 0, r.pfadd("foos", "baz")
  assert_equal 1, r.pfadd("foos", "foobar", "foobaz")
  assert_equal 0, r.pfadd("foos", "foobar", "foobaz")
  assert_raise(ArgumentError) {r.pfadd }
end

assert("Redis#pfcount") do
  r = Redis.new HOST, PORT
  r.flushall
  r.pfadd("foos", "bar")
  r.pfadd("foos", "baz")
  r.pfadd("bars", "foobar")

  assert_equal 2, r.pfcount("foos")
  assert_equal 3, r.pfcount("foos", "bars")
  assert_equal 3, r.pfcount("foos", "bars", "barz")
  assert_raise(ArgumentError) {r.pfcount }
end

assert("Redis#pfmerge") do
  r = Redis.new HOST, PORT
  r.flushall
  r.pfadd("foo", "a", "b", "c")
  r.pfadd("bar", "c", "d", "e")
  r.pfadd("baz", "e", "f", "g")

  assert_raise(ArgumentError) {r.pfmerge }
  assert_raise(ArgumentError) {r.pfmerge "foobar" }

  r.pfmerge "foos", "foo"
  r.pfmerge "foobar", "foo", "bar"
  r.pfmerge "foobarbaz", "foo", "bar", "baz", "foobar"

  assert_equal 3, r.pfcount("foos")
  assert_equal 5, r.pfcount("foobar")
  assert_equal 7, r.pfcount("foobarbaz")
end

assert("Redis#multi") do
  client1 = Redis.new HOST, PORT
  client2 = Redis.new HOST, PORT
  client1.del "hoge"

  ret1 = client1.multi
  client1.set "hoge", "fuga"
  ret2 = client2.get "hoge"

  assert_equal "OK", ret1
  assert_equal nil, ret2

  client1.discard
end

assert("Redis#exec") do
  client1 = Redis.new HOST, PORT
  client2 = Redis.new HOST, PORT
  client1.del "hoge"

  client1.multi
  client1.set "hoge", "fuga"
  ret1 = client1.exec
  ret2 = client2.get "hoge"

  assert_equal ["OK"], ret1
  assert_equal "fuga", ret2
end

assert("Redis#discard") do
  client1 = Redis.new HOST, PORT
  client2 = Redis.new HOST, PORT
  client1.del "hoge"

  client1.multi
  client1.set "hoge", "fuga"
  ret1 = client1.discard
  ret2 = client2.get "hoge"
  ret3 = client1.discard

  assert_equal "OK", ret1
  assert_equal nil, ret2
  assert_not_equal "OK", ret3
end

assert("Redis#watch") do
  client1 = Redis.new HOST, PORT
  client2 = Redis.new HOST, PORT
  client1.set "hoge", "1"
  client1.set "fuga", "2"

  ret1 = client1.watch "hoge", "fuga"
  client1.multi
  client1.set "hoge", "10"
  client1.set "fuga", "20"
  ret2 = client1.exec

  client1.watch "hoge", "fuga"
  client1.multi
  client1.set "hoge", "100"
  client1.set "fuga", "200"
  client2.set "hoge", "-100"
  ret3 = client1.exec
  ret4 = client1.get "hoge"

  assert_equal "OK", ret1
  assert_equal ["OK", "OK"], ret2
  assert_equal nil, ret3
  assert_equal "-100", ret4
end

assert("Redis#unwatch") do
  client1 = Redis.new HOST, PORT
  client2 = Redis.new HOST, PORT
  client1.set "hoge", "1"
  client1.set "fuga", "2"

  client1.watch "hoge", "fuga"
  ret1 = client1.unwatch
  client1.multi
  client1.set "hoge", "100"
  client1.set "fuga", "200"
  client2.set "hoge", "-100"
  ret2 = client1.exec

  assert_equal "OK", ret1
  assert_not_equal nil, ret2
end

assert("Redis#auth") do
  r = Redis.new HOST, SECURED_PORT
  res = begin
          r.ping
        rescue => e
          e
        end
  assert_equal "NOAUTH Authentication required.", res

  res = begin
          r.auth(nil)
        rescue Redis::AuthError => e
          e
        end
  assert_equal "password should be a string", res.message

  res = begin
        r.auth("wrong secret")
      rescue => e
        e
      end
  assert_kind_of(Redis::AuthError, res)
  assert_equal "incorrect password", res.message

  assert_equal r.auth("secret"), r
  assert_equal "PONG", r.ping
end
