host    = "127.0.0.1"
port    = 6379

puts("> redis connect " + host + ":" + port.to_s)
r = Redis.new(host, port)

key     = "hoge"

puts("> redis set " + key + " 200")
r.set(key, "200")

puts("> redis get " + key)
puts(key + ": " + r.get(key))

puts("> redis set " + key + " fuga")
r[key] =  "fuga"

puts("> redis get " + key)
puts(key + ": " + r[key])

puts("> redis del " + key)
r.del(key)

if r[key].nil?
    puts "del success!"
end

puts("> redis incr " + key)
puts "#{key} incr: #{r.incr(key)}"
puts "#{key} incr: #{r.incr(key)}"
puts "#{key} incr: #{r.incr(key)}"
puts "#{key} incr: #{r.incr(key)}"

puts("> redis decr " + key)
puts "#{key} decr: #{r.decr(key)}"
puts "#{key} decr: #{r.decr(key)}"
puts "#{key} decr: #{r.decr(key)}"
puts "#{key} decr: #{r.decr(key)}"

r.close
