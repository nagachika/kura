# encoding: utf-8

require "google/apis/bigquery_v2"

class Google::Apis::BigqueryV2::Job
  attr_accessor :kura_api

  def wait(timeout, &blk)
    kura_api.wait_job(self, timeout, &blk)
  end

  def cancel
    kura_api.cancel_job(self)
  end

  def inspect
    "<##{self.class} job_id=#{self.job_reference.job_id rescue nil}>"
  end
end

