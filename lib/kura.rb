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

  # == Kura.client
  # Create Kura::Client object with GCP credential.
  #
  #     Kura.client(json_key_file)
  #     Kura.client(json_key_hash)
  #     Kura.client(gcp_project_id, email_address, prm_file)
  #     Kura.client(gcp_project_id, email_address, prm_file_contents)
  #     Kura.client(gcp_project_id, email_address, private_key_object)
  #
  def self.client(project_id=nil, email_address=nil, private_key=nil, http_options: {timeout: 60})
    if email_address.nil? and private_key.nil?
      if project_id.is_a?(String)
        credential = JSON.parse(File.binread(project_id))
      elsif project_id.is_a?(Hash)
        credential = project_id
      else
        raise ArgumentError, "#{self.class.name}.client accept JSON credential file path or decoded Hash object."
      end
      project_id = credential["project_id"]
      email_address = credential["client_email"]
      private_key = get_private_key(credential["private_key"])
    elsif private_key
      private_key = get_private_key(private_key)
    end
    self::Client.new(default_project_id: project_id, email_address: email_address, private_key: private_key, http_options: http_options)
  end
end
