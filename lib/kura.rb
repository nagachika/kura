# coding: utf-8

require "kura/version"
require "kura/client"

module Kura
  class ApiError < RuntimeError
    def initialize(reason, message)
      @reason = reason
      super(message)
    end
    attr_reader :reason
  end

  class TimeoutError < RuntimeError
  end

  def self.get_private_key(private_key)
    private_key_file = nil
    private_key_content = nil
    if private_key.respond_to?(:to_path)
      private_key_file = private_key.to_path
    elsif private_key.is_a?(String) and File.readable?(private_key)
      private_key_file = private_key
    end
    if private_key_file
      private_key_content = File.binread(private_key_file)
    else
      private_key_content = private_key
    end
    if private_key_content
      private_key = OpenSSL::PKey.read(private_key_content)
    end
    private_key
  end

  def self.client(project_id, email_address, private_key)
    private_key = get_private_key(private_key)
    self::Client.new(project_id, email_address, private_key)
  end
end
