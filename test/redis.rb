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

# TODO: Add test
# - randomkey
# - del
# - incr
# - decr
# - lpush
# - lrange
# - ltrim
# - publish
