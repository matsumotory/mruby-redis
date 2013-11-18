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

puts "> redis publish :one hello"
r.publish "one", "hello"

r.close
