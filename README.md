redis class for mruby


```ruby
host    = "127.0.0.1"
port    = 6379

puts("> redis connect " + host + ":" + port.to_s + "<br>")
redis = Apache::Redis.new(host, port)

key     = "hoge"
val     = "100"

puts("> redis set " + key + " " + val + "<br>")
redis.set(key, val)

puts("> redis get " + key + "<br>")
puts(key + ": " + redis.get(key) + "<br><br>")

```
