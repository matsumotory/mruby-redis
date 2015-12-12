unless Object.const_defined?("IOError")
  class IOError < StandardError; end
end

unless Object.const_defined?("EOFError")
  class EOFError < IOError; end
end
