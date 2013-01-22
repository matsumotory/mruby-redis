MRuby::Gem::Specification.new('mruby-redis') do |spec|
  spec.license = 'MIT'
  spec.authors = 'MATSUMOTO Ryosuke'
  spec.linker.libraries << "hiredis"
end
