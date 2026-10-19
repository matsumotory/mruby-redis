MRuby::Gem::Specification.new('mruby-redis') do |spec|
  spec.license = 'MIT'
  spec.authors = 'MATSUMOTO Ryosuke'
  spec.version = '0.0.1'
  # for expire test
  require 'open3'

  # The hiredis release that this gem is built and tested with. The build
  # clones this tag, so a new commit on the default branch of hiredis cannot
  # change or break the build. To build with another hiredis tag, set
  # MRUBY_REDIS_HIREDIS_VERSION in the environment of rake, or in the build
  # configuration before the conf.gem line (see README.md).
  hiredis_version = ENV['MRUBY_REDIS_HIREDIS_VERSION'] || 'v1.5.0'
  unless hiredis_version =~ /\A[A-Za-z0-9][A-Za-z0-9._-]*\z/
    fail "mruby-redis: MRUBY_REDIS_HIREDIS_VERSION must be a hiredis tag such as v1.5.0, not #{hiredis_version.inspect}"
  end
  # The directory name carries the version, so that a build directory left by
  # another version is not reused.
  hiredis_dir = "#{build_dir}/hiredis-#{hiredis_version}"

  def run_command env, command
    STDOUT.sync = true
    puts "build: [exec] #{command}"
    Open3.popen2e(env, command) do |stdin, stdout, thread|
      print stdout.read
      fail "#{command} failed" if thread.value != 0
    end
  end

  FileUtils.mkdir_p build_dir

  if ! File.exist? hiredis_dir
    run_command({}, "git clone --depth 1 --branch #{hiredis_version} https://github.com/redis/hiredis.git #{hiredis_dir}")
  end

  if ! File.exist? "#{hiredis_dir}/libhiredis.a"
    Dir.chdir hiredis_dir do
      # hiredis is compiled with the flags of the mruby build, and its
      # Makefile adds -Werror (USE_WERROR=1 by default). The gcc toolchain of
      # mruby passes -Wundef, and ffc.h of hiredis 1.4.0 and later tests the
      # undefined macro FFC_DEBUG with #if, so that warning would stop the
      # build. -Wno-error=undef keeps it a warning, for hiredis only.
      cflags = spec.build.cc.flags.flatten.reject {|flag| flag == '-fPIE'} + ['-Wno-error=undef']
      e = {
        'CC' => "#{spec.build.cc.command} #{cflags.join(' ')}",
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
