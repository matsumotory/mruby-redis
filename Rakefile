MRUBY_CONFIG=File.expand_path(ENV["MRUBY_CONFIG"] || "build_config.rb")
# The mruby release (a tag of mruby/mruby) that `rake test` builds against.
MRUBY_VERSION=ENV["MRUBY_VERSION"] || "3.3.0"

file :mruby do
  sh "git clone --depth 1 --branch #{MRUBY_VERSION} https://github.com/mruby/mruby.git mruby"
end

desc "compile binary"
task :compile => :mruby do
  sh "cd mruby && MRUBY_CONFIG=#{MRUBY_CONFIG} rake all"
end

desc "test"
task :test => :mruby do
  sh "cd mruby && MRUBY_CONFIG=#{MRUBY_CONFIG} rake all test"
end

desc "cleanup"
task :clean do
  sh "cd mruby && rake deep_clean"
end

task :default => :test
