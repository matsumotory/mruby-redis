MRuby::Gem::Specification.new('mruby-redis') do |spec|
  spec.license = 'MIT'
  spec.authors = 'MATSUMOTO Ryosuke'
  spec.version = '0.0.1'
  spec.linker.libraries << "hiredis"
  # for expire test
  spec.add_dependency "mruby-sleep"
end
