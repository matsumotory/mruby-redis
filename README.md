redis class for mruby

## install

        git clone git://github.com/matsumoto-r/mruby-redis.git
        cd mruby-redis
        make
        cd example
        make
        ./tester redis.rb


## redis.rb

* code


```ruby
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
```

* execute

```test
> redis connect 127.0.0.1:6379
> redis set hoge 200
> redis get hoge
hoge: 200
```
