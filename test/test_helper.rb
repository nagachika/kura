# simplecov must be loaded before any of target code
if ENV['COVERAGE']
  require 'simplecov'
  unless SimpleCov.running
    SimpleCov.start do
      add_filter '/test/'
      add_filter '/gems/'
    end
  end
end

require "bundler"

begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

require 'test/unit'
require 'test/unit/power_assert'

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'kura'
