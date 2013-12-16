host     = "127.0.0.1"
port     = 6379
key      = "hoge"
database = 0

puts "> redis connect #{host}: #{port.to_s}"
r = Redis.new host, port

puts "> redis select: #{database}"
r.select database

puts "> redis set #{key} 200"
r.set key, "200"

puts "> redis get #{key}"
puts "#{key}: #{r[key]}"

puts "> redis exists #{key}"
puts "#{r.exists?(key)}"

puts "> redis exists fuga"
puts "#{r.exists?("fuga")}"

puts "> redis set #{key} fuga"
r[key] =  "fuga"

puts "> redis get #{key}"
puts "#{key}: #{r.get key}"

puts "> redis del #{key}"
r.del key

if r[key].nil?
    puts "del success!"
end

puts "> redis incr #{key}"
puts "#{key} incr: #{r.incr(key)}"
puts "#{key} incr: #{r.incr(key)}"
puts "#{key} incr: #{r.incr(key)}"
puts "#{key} incr: #{r.incr(key)}"

puts "> redis decr #{key}"
puts "#{key} decr: #{r.decr(key)}"
puts "#{key} decr: #{r.decr(key)}"
puts "#{key} decr: #{r.decr(key)}"
puts "#{key} decr: #{r.decr(key)}"

puts "> redis lpush logs error"
r.lpush "logs", "error1"
r.lpush "logs", "error2"
r.lpush "logs", "error3"

puts "> redis lrange 0 -1"
puts r.lrange "logs", 0, -1

puts "> redis ltrim 1 -1"
r.ltrim "logs", 1, -1

puts "> redis lrange 0 -1"
puts r.lrange "logs", 0, -1

puts "> redis del logs"
r.del "logs"

if r["logs"].nil?
    puts "del success!"
end

puts "> redis hset myhash field1 a"
r.hset "myhash", "field1", "a"

puts "> redis hset myhash field2 b"
r.hset "myhash", "field2", "b"

puts "> redis hget myhash field1"
puts r.hget "myhash", "field1"

puts "> redis hget myhash field2"
puts r.hget "myhash", "field2"

puts "> redis hdel myhash field1"
puts r.hdel "myhash", "field1"

puts "> redis del myhash"

if r["myhash"].nil?
    puts "del success!"
end

puts "> redis zadd hs 80 a"
r.zadd "hs", 80, "a"

puts "> redis zadd hs 50.1 b"
r.zadd "hs", 50.1, "b"

puts "> redis zadd hs 60 c"
r.zadd "hs", 60, "c"

puts "> redis zscore hs a"
puts r.zscore "hs", "a"

puts "> redis zrange hs 0 -1"
puts r.zrange "hs", 0, -1

puts "> redis zrank hs b"
puts r.zrank "hs", "b"

puts "> redis zrank hs c"
puts r.zrank "hs", "c"

puts "> redis zrank hs a"
puts r.zrank "hs", "a"

puts ">redis zrevrange hs 0 -1"
puts r.zrevrange "hs", 0, -1

puts ">redis zrevrank hs a"
puts r.zrevrank "hs", "a"

puts ">redis zrevrank hs c"
puts r.zrevrank "hs", "c"

puts ">redis zrevrank hs b"
puts r.zrevrank "hs", "b"

puts "> redis del hs"
r.del "hs"
if r["hs"].nil?
    puts "del success!"
end

puts "> redis publish :one hello"
r.publish "one", "hello"

r.close
