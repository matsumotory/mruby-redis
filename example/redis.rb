host    = "127.0.0.1"
port    = 6379

puts("> redis connect " + host + ":" + port.to_s)
redis = Redis::Simple.new(host, port)

key     = "hoge"
val     = "200"

puts("> redis set " + key + " " + val)
redis.set(key, val)

puts("> redis get " + key)
puts(key + ": " + redis.get(key))

redis.close
