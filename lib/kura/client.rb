# coding: utf-8

require "google/api_client"
require "kura/version"

module Kura
  class Client
    def initialize(project_id, email_address, private_key)
      @project_id = project_id
      @scope = "https://www.googleapis.com/auth/bigquery"
      @email_address = email_address
      @private_key = private_key
      auth = Signet::OAuth2::Client.new(
        token_credential_uri: "https://accounts.google.com/o/oauth2/token",
        audience: "https://accounts.google.com/o/oauth2/token",
        scope: @scope,
        issuer: @email_address,
        signing_key: @private_key)
      @api = Google::APIClient.new(application_name: "Kura", application_version: Kura::VERSION, authorization: auth)
      @api.authorization.fetch_access_token!
      @bigquery_api = @api.discovered_api("bigquery", "v2")
    end

    def datasets(project_id: @project_id, all: false, limit: 1000)
      r = @api.execute(api_method: @bigquery_api.datasets.list, parameters: { projectId: project_id, all: all, maxResult: limit })
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data.datasets
    end

    def dataset(dataset_id, project_id: @project_id)
      r = @api.execute(api_method: @bigquery_api.datasets.get, parameters: { projectId: project_id, datasetId: dataset_id })
      unless r.success?
        if r.data.error["code"] == 404
          return nil
        else
          error = r.data["error"]["errors"][0]
          raise Kura::ApiError.new(error["reason"], error["message"])
        end
      end
      r.data
    end

    def insert_dataset(dataset_id)
      r = @api.execute(api_method: @bigquery_api.datasets.insert, parameters: { projectId: @project_id }, body_object: { datasetReference: { datasetId: dataset_id } })
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data
    end

    def delete_dataset(dataset_id, delete_contents: false)
      r = @api.execute(api_method: @bigquery_api.datasets.delete, parameters: { projectId: @project_id, datasetId: dataset_id, deleteContents: delete_contents })
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data
    end

    def patch_dataset(dataset_id, project_id: @project_id, access: nil, description: nil, default_table_expiration_ms: nil, friendly_name: nil )
      body = {}
      body["access"] = access if access
      body["defaultTableExpirationMs"] = default_table_expiration_ms if default_table_expiration_ms
      body["description"] = description if description
      body["friendlyName"] = friendly_name if friendly_name
      r = @api.execute(api_method: @bigquery_api.datasets.patch, parameters: { projectId: project_id, datasetId: dataset_id }, body_object: body)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data
    end

    def tables(dataset_id, project_id: @project_id, limit: 1000)
      params = { projectId: project_id, datasetId: dataset_id, maxResult: limit }
      r = @api.execute(api_method: @bigquery_api.tables.list, parameters: params)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data.tables
    end

    def table(dataset_id, table_id, project_id: @project_id)
      params = { projectId: project_id, datasetId: dataset_id, tableId: table_id }
      r = @api.execute(api_method: @bigquery_api.tables.get, parameters: params)
      unless r.success?
        if r.data["error"]["code"] == 404
          return nil
        else
          error = r.data["error"]["errors"][0]
          raise Kura::ApiError.new(error["reason"], error["message"])
        end
      end
      r.data
    end

    def delete_table(dataset_id, table_id)
      params = { projectId: @project_id, datasetId: dataset_id, tableId: table_id }
      r = @api.execute(api_method: @bigquery_api.tables.delete, parameters: params)
      unless r.success?
        if r.data["error"]["code"] == 404
          return nil
        else
          error = r.data["error"]["errors"][0]
          raise Kura::ApiError.new(error["reason"], error["message"])
        end
      end
      r.data
    end

    def mode_to_write_disposition(mode)
      unless %i{ append truncate empty }.include?(mode)
        raise "mode option should be one of :append, :truncate, :empty but #{mode}"
      end
      "WRITE_#{mode.to_s.upcase}"
    end
    private :mode_to_write_disposition

    def insert_job(configuration, wait: nil)
      params = { projectId: @project_id }
      body = { configuration: configuration }
      r = @api.execute(api_method: @bigquery_api.jobs.insert, parameters: params, body_object: body)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      if wait
        wait_job(r.data.jobReference.jobId, wait)
      else
        r.data.jobReference.jobId
      end
    end

    def query(dataset_id, table_id, sql, mode: :truncate, allow_large_result: true, wait: nil)
      write_disposition = mode_to_write_disposition(mode)
      configuration = {
        query: {
          query: sql,
          destinationTable: { projectId: @project_id, datasetId: dataset_id, tableId: table_id },
          writeDisposition: write_disposition,
          allowLargeResults: allow_large_result,
        }
      }
      insert_job(configuration, wait: wait)
    end

    def load(dataset_id, table_id, source_uris, schema, delimiter: ",", mode: :append, wait: nil)
      write_disposition = mode_to_write_disposition(mode)
      source_uris = [source_uris] if source_uris.is_a?(String)
      configuration = {
        load: {
          sourceUris: source_uris,
          destinationTable: {
            projectId: @project_id,
            datasetId: dataset_id,
            tableId: table_id,
          },
          fieldDelimiter: delimiter,
          writeDisposition: write_disposition,
          schema: { fields: schema },
        }
      }
      insert_job(configuration, wait: wait)
    end

    def extract(dataset_id, table_id, dest_uris, wait: nil)
      dest_uris = [ dest_uris ] if dest_uris.is_a?(String)
      configuration = {
        extract: {
          sourceTable: {
            projectId: @project_id,
            datasetId: dataset_id,
            tableId: table_id,
          },
          destinationUris: dest_uris,
        }
      }
      insert_job(configuration, wait: wait)
    end

    def copy(src_dataset_id, src_table_id, dest_dataset_id, dest_table_id, mode: :truncate, wait: nil)
      write_disposition = mode_to_write_disposition(mode)
      configuration = {
        copy: {
          destinationTable: {
            projectId: @project_id,
            datasetId: dest_dataset_id,
            tableId: dest_table_id,
          },
          sourceTable: {
            projectId: @project_id,
            datasetId: src_dataset_id,
            tableId: src_table_id,
          },
          writeDisposition: write_disposition,
        }
      }
      insert_job(configuration, wait: wait)
    end

    def job(job_id)
      params = { projectId: @project_id, jobId: job_id }
      r = @api.execute(api_method: @bigquery_api.jobs.get, parameters: params)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data
    end

    def job_finished?(job_id)
      r = job(job_id)
      if r.status.state == "DONE"
        if r.status["errorResult"]
          raise Kura::ApiError.new(r.status.errorResult.reason, r.status.errorResult.message)
        end
        return true
      end
      return false
    end

    def wait_job(job_id, timeout=60*10)
      expire = Time.now + timeout
      while expire > Time.now
        if job_finished?(job_id)
          return true
        end
        if block_given?
          yield
        end
        sleep 1
      end
      raise Kura::TimeoutError, "wait job timeout"
    end
  end
end
