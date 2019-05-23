MRuby::Gem::Specification.new('mruby-redis') do |spec|
  spec.license = 'MIT'
  spec.authors = 'MATSUMOTO Ryosuke'
  spec.version = '0.0.1'
  # for expire test
  require 'open3'

  hiredis_dir = "#{build_dir}/hiredis"

  def run_command env, command
    STDOUT.sync = true
    puts "build: [exec] #{command}"
    Open3.popen2e(env, command) do |stdin, stdout, thread|
      print stdout.read
      fail "#{command} failed" if thread.value != 0
    end
  end

  FileUtils.mkdir_p build_dir

  if ! File.exists? hiredis_dir
    Dir.chdir(build_dir) do
      e = {}
      run_command e, 'git clone git://github.com/redis/hiredis.git'
      # Latest HIREDIS is not compatible for OS X
      run_command e, "git --git-dir=#{hiredis_dir}/.git --work-tree=#{hiredis_dir} checkout v0.13.3" if `uname` =~ /Darwin/
    end
  end

  if ! File.exists? "#{hiredis_dir}/libhiredis.a"
    Dir.chdir hiredis_dir do
      e = {
        'CC' => "#{spec.build.cc.command} #{spec.build.cc.flags.reject {|flag| flag == '-fPIE'}.join(' ')}",
        'CXX' => "#{spec.build.cxx.command} #{spec.build.cxx.flags.join(' ')}",
        'LD' => "#{spec.build.linker.command} #{spec.build.linker.flags.join(' ')}",
        'AR' => spec.build.archiver.command,
        'PREFIX' => hiredis_dir
      }
      make_command = `uname` =~ /BSD/ ? "gmake" : "make"
      run_command e, "#{make_command}"
      run_command e, "#{make_command} install"
    end
  end

  spec.cc.include_paths << "#{hiredis_dir}/include"
  spec.linker.flags_before_libraries << "#{hiredis_dir}/lib/libhiredis.a"

  spec.add_dependency "mruby-sleep"
  spec.add_dependency "mruby-pointer", :github => 'matsumotory/mruby-pointer'
end
