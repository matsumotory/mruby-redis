##
## Redis Test
##

assert("Redis#set, Redis#get") do
  r = Redis.new "127.0.0.1", 6379
  r.set "hoge", "fuga"
  r.get("hoge") == "fuga"
end

assert("Redis#[]=, Redis#[]") do
  r = Redis.new "127.0.0.1", 6379
  r["hoge"] = "fuga"
  r["hoge"] == "fuga"
end

assert("Redis#exist?") do
  r = Redis.new "127.0.0.1", 6379
  r["hoge"] = "aaa"
  assert_true(r.exists?("hoge"))
  assert_false(r.exists?("fuga"))
end
