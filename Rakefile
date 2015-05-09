MRUBY_CONFIG=File.expand_path(ENV["MRUBY_CONFIG"] || "build_config.rb")
MRUBY_VERSION=ENV["MRUBY_VERSION"] || "1.1.0"

file :mruby do
  sh "wget -O mruby.tar.gz https://github.com/mruby/mruby/archive/#{MRUBY_VERSION}.tar.gz"
  sh "tar -xvzf mruby.tar.gz"
  sh "rm mruby.tar.gz"
  sh "mv mruby-#{MRUBY_VERSION} mruby"
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
