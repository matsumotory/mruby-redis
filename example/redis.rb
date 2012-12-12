host    = "127.0.0.1"
port    = 6379

puts("> redis connect " + host + ":" + port.to_s)
redis = Redis.new(host, port)

key     = "hoge"

puts("> redis set " + key + " 200")
redis.set(key, "200")

puts("> redis get " + key)
puts(key + ": " + redis.get(key))

puts("> redis set " + key + " fuga")
redis[key] =  "fuga"

puts("> redis get " + key)
puts(key + ": " + redis[key])

puts("> redis del " + key)
redis.del(key)

if redis[key].nil?
    puts "del success!"
end

puts("> redis incr " + key)
puts "#{key} incr: #{redis.incr(key)}"
puts "#{key} incr: #{redis.incr(key)}"
puts "#{key} incr: #{redis.incr(key)}"
puts "#{key} incr: #{redis.incr(key)}"

puts("> redis decr " + key)
puts "#{key} decr: #{redis.decr(key)}"
puts "#{key} decr: #{redis.decr(key)}"
puts "#{key} decr: #{redis.decr(key)}"
puts "#{key} decr: #{redis.decr(key)}"

redis.close
