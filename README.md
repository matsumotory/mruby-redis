# redis class for mruby

## install by mrbgems
 - add conf.gem line to `build_config.rb`
```ruby
MRuby::Build.new do |conf|

    # ... (snip) ...

    conf.gem :git => 'https://github.com/matsumoto-r/mruby-redis.git'
end
```


## redis.rb

* code


```ruby
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
```

* execute

```text
> redis connect 127.0.0.1: 6379
> redis select: 0
> redis set hoge 200
> redis get hoge
hoge: 200
> redis set hoge fuga
> redis get hoge
hoge: fuga
> redis del hoge
del success!
> redis incr hoge
hoge incr: 1
hoge incr: 2
hoge incr: 3
hoge incr: 4
> redis decr hoge
hoge decr: 3
hoge decr: 2
hoge decr: 1
hoge decr: 0
> redis lpush logs error
> redis lrange 0 -1
["error3", "error2", "error1"]
> redis ltrim 1 -1
> redis lrange 0 -1
["error2", "error1"]
> redis del logs
del success!
> redis publish :one hello
```
