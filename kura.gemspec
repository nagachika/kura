# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kura/version'

Gem::Specification.new do |spec|
  spec.name          = "kura"
  spec.version       = Kura::VERSION
  spec.authors       = ["Chikanaga Tomoyuki"]
  spec.email         = ["nagachika@ruby-lang.org"]

  spec.summary       = %q{Interface to BigQuery API v2.}
  spec.description   = %q{Kura is an interface to BigQuery API v2. It is a wrapper of google-api-client.}
  spec.homepage      = "https://github.com/nagachika/kura/"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.has_rdoc      = false

  spec.required_ruby_version = '>= 2.1'

  spec.add_runtime_dependency "google-api-client", ">= 0.9.11"

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "simplecov"
  spec.add_development_dependency "test-unit-power_assert"
  spec.add_development_dependency "pry"
end
