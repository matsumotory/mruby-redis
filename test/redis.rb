##
## Redis Test
##

assert("Redis#set, Redis#get") do
  r = Redis.new "127.0.0.1", 6379
  r.set "hoge", "fuga"
  assert_equal("fuga", r.get("hoge"))
end

assert("Redis#[]=, Redis#[]") do
  r = Redis.new "127.0.0.1", 6379
  r["hoge"] = "fuga"
  assert_equal("fuga", r["hoge"])
end

assert("Redis#select") do
  r = Redis.new "127.0.0.1", 6379
  assert_equal(r, r.select(0))
end

assert("Redis#exist?") do
  r = Redis.new "127.0.0.1", 6379
  r["hoge"] = "aaa"
  assert_true(r.exists?("hoge"))
  assert_false(r.exists?("fuga"))
end

assert("Redis#zadd, Redis#zrange") do
  r = Redis.new "127.0.0.1", 6379
  r.del("hs")
  r.zadd("hs", 80, "a")
  r.zadd("hs", 50.1, "b")
  r.zadd("hs", 60, "c")
  assert_equal(["b", "c", "a"], r.zrange("hs", 0, -1))
end

assert("Redis#zrevrange") do
  r = Redis.new "127.0.0.1", 6379
  r.del("hs")
  r.zadd("hs", 80, "a")
  r.zadd("hs", 50.1, "b")
  r.zadd("hs", 60, "c")
  assert_equal(["a", "c", "b"], r.zrevrange("hs", 0, -1))
end

assert("Redis#zrank") do
  r = Redis.new "127.0.0.1", 6379
  r.del("hs")
  r.zadd("hs", 80, "a")
  r.zadd("hs", 50.1, "b")
  r.zadd("hs", 60, "c")
  assert_equal(0, r.zrank("hs", "b"))
  assert_equal(1, r.zrank("hs", "c"))
  assert_equal(2, r.zrank("hs", "a"))
end

# TODO: Add test
# - randomkey
# - del
# - incr
# - decr
# - lpush
# - lrange
# - ltrim
# - publish
