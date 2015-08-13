# coding: utf-8

require "google/api_client"
require "kura/version"

module Kura
  class Client
    def initialize(default_project_id: nil, email_address: nil, private_key: nil, http_options: {open_timeout: 60})
      @default_project_id = default_project_id
      @scope = "https://www.googleapis.com/auth/bigquery"
      @email_address = email_address
      @private_key = private_key
      if @email_address and @private_key
        auth = Signet::OAuth2::Client.new(
          token_credential_uri: "https://accounts.google.com/o/oauth2/token",
          audience: "https://accounts.google.com/o/oauth2/token",
          scope: @scope,
          issuer: @email_address,
          signing_key: @private_key)
      else
        auth = Google::APIClient::ComputeServiceAccount.new
      end
      @api = Google::APIClient.new(application_name: "Kura", application_version: Kura::VERSION, authorization: auth, faraday_option: http_options)
      @api.authorization.fetch_access_token!
      @bigquery_api = @api.discovered_api("bigquery", "v2")

      if @default_project_id.nil?
        @default_project_id = self.projects.first.id
      end
    end

    def projects(limit: 1000)
      r = @api.execute(api_method: @bigquery_api.projects.list, parameters: { maxResults: limit })
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data.projects
    end

    def datasets(project_id: @default_project_id, all: false, limit: 1000)
      r = @api.execute(api_method: @bigquery_api.datasets.list, parameters: { projectId: project_id, all: all, maxResult: limit })
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data.datasets
    end

    def dataset(dataset_id, project_id: @default_project_id)
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

    def insert_dataset(dataset_id, project_id: @default_project_id)
      r = @api.execute(api_method: @bigquery_api.datasets.insert, parameters: { projectId: project_id }, body_object: { datasetReference: { datasetId: dataset_id } })
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data
    end

    def delete_dataset(dataset_id, project_id: @default_project_id, delete_contents: false)
      r = @api.execute(api_method: @bigquery_api.datasets.delete, parameters: { projectId: project_id, datasetId: dataset_id, deleteContents: delete_contents })
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data
    end

    def patch_dataset(dataset_id, project_id: @default_project_id, access: nil, description: nil, default_table_expiration_ms: nil, friendly_name: nil )
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

    def tables(dataset_id, project_id: @default_project_id, limit: 1000)
      params = { projectId: project_id, datasetId: dataset_id, maxResult: limit }
      r = @api.execute(api_method: @bigquery_api.tables.list, parameters: params)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data.tables
    end

    def table(dataset_id, table_id, project_id: @default_project_id)
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

    def delete_table(dataset_id, table_id, project_id: @default_project_id)
      params = { projectId: project_id, datasetId: dataset_id, tableId: table_id }
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

    def list_tabledata(dataset_id, table_id, project_id: @default_project_id, start_index: 0, max_result: 100, page_token: nil, schema: nil)
      schema ||= table(dataset_id, table_id, project_id: project_id).schema.fields
      field_names = schema.map{|f| f["name"] }
      params = { projectId: project_id, datasetId: dataset_id, tableId: table_id, maxResults: max_result }
      if page_token
        params[:pageToken] = page_token
      else
        params[:startIndex] = start_index
      end
      r = @api.execute(api_method: @bigquery_api.tabledata.list, parameters: params)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      {
        total_rows: r.data.totalRows,
        next_token: r.data["pageToken"],
        rows: r.data.rows.map do |row|
          row.f.zip(field_names).each_with_object({}) do |(v, fn), tbl| tbl[fn] = v.v end
        end
      }
    end

    def mode_to_write_disposition(mode)
      unless %i{ append truncate empty }.include?(mode)
        raise "mode option should be one of :append, :truncate, :empty but #{mode}"
      end
      "WRITE_#{mode.to_s.upcase}"
    end
    private :mode_to_write_disposition

    def insert_job(configuration, project_id: @default_project_id, media: nil, wait: nil)
      params = { projectId: project_id }
      if media
        params["uploadType"] = "multipart"
      end
      body = { configuration: configuration }
      r = @api.execute(api_method: @bigquery_api.jobs.insert, parameters: params, body_object: body, media: media)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      if wait
        wait_job(r.data.jobReference.jobId, wait, project_id: project_id)
      else
        r.data.jobReference.jobId
      end
    end

    def query(sql, mode: :truncate,
              dataset_id: nil, table_id: nil,
              allow_large_result: true, # for backward compatibility
              allow_large_results: allow_large_result,
              flatten_results: true,
              priority: "INTERACTIVE",
              use_query_cache: true,
              project_id: @default_project_id,
              job_project_id: @default_project_id,
              wait: nil)
      write_disposition = mode_to_write_disposition(mode)
      configuration = {
        query: {
          query: sql,
          writeDisposition: write_disposition,
          allowLargeResults: allow_large_results,
          flattenResults: flatten_results,
          priority: priority,
          useQueryCache: use_query_cache,
        }
      }
      if dataset_id and table_id
        configuration[:query][:destinationTable] = { projectId: project_id, datasetId: dataset_id, tableId: table_id }
      end
      insert_job(configuration, wait: wait, project_id: job_project_id)
    end

    def load(dataset_id, table_id, source_uris=nil,
             schema: nil, delimiter: ",", field_delimiter: delimiter, mode: :append,
             allow_jagged_rows: false, max_bad_records: 0,
             ignore_unknown_values: false,
             allow_quoted_newlines: false,
             quote: '"', skip_leading_rows: 0,
             source_format: "CSV",
             project_id: @default_project_id,
             job_project_id: @default_project_id,
             file: nil, wait: nil)
      write_disposition = mode_to_write_disposition(mode)
      source_uris = [source_uris] if source_uris.is_a?(String)
      configuration = {
        load: {
          destinationTable: {
            projectId: project_id,
            datasetId: dataset_id,
            tableId: table_id,
          },
          writeDisposition: write_disposition,
          allowJaggedRows: allow_jagged_rows,
          maxBadRecords: max_bad_records,
          ignoreUnknownValues: ignore_unknown_values,
          sourceFormat: source_format,
        }
      }
      if schema
        configuration[:load][:schema] = { fields: schema }
      end
      if source_format == "CSV"
        configuration[:load][:fieldDelimiter] = field_delimiter
        configuration[:load][:allowQuotedNewlines] = allow_quoted_newlines
        configuration[:load][:quote] = quote
        configuration[:load][:skipLeadingRows] = skip_leading_rows
      end
      if file
        file = Google::APIClient::UploadIO.new(file, "application/octet-stream")
      else
        configuration[:load][:sourceUris] = source_uris
      end
      insert_job(configuration, media: file, wait: wait, project_id: job_project_id)
    end

    def extract(dataset_id, table_id, dest_uris,
                compression: "NONE",
                destination_format: "CSV",
                field_delimiter: ",",
                print_header: true,
                project_id: @default_project_id,
                job_project_id: @default_project_id,
                wait: nil)
      dest_uris = [ dest_uris ] if dest_uris.is_a?(String)
      configuration = {
        extract: {
          compression: compression,
          destinationFormat: destination_format,
          sourceTable: {
            projectId: project_id,
            datasetId: dataset_id,
            tableId: table_id,
          },
          destinationUris: dest_uris,
        }
      }
      if destination_format == "CSV"
        configuration[:extract][:fieldDelimiter] = field_delimiter
        configuration[:extract][:printHeader] = print_header
      end
      insert_job(configuration, wait: wait, project_id: job_project_id)
    end

    def copy(src_dataset_id, src_table_id, dest_dataset_id, dest_table_id,
             mode: :truncate,
             src_project_id: @default_project_id,
             dest_project_id: @default_project_id,
             job_project_id: @default_project_id,
             wait: nil)
      write_disposition = mode_to_write_disposition(mode)
      configuration = {
        copy: {
          destinationTable: {
            projectId: dest_project_id,
            datasetId: dest_dataset_id,
            tableId: dest_table_id,
          },
          sourceTable: {
            projectId: src_project_id,
            datasetId: src_dataset_id,
            tableId: src_table_id,
          },
          writeDisposition: write_disposition,
        }
      }
      insert_job(configuration, wait: wait, project_id: job_project_id)
    end

    def job(job_id, project_id: @default_project_id)
      params = { projectId: project_id, jobId: job_id }
      r = @api.execute(api_method: @bigquery_api.jobs.get, parameters: params)
      unless r.success?
        error = r.data["error"]["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      end
      r.data
    end

    def job_finished?(r)
      if r.status.state == "DONE"
        if r.status["errorResult"]
          raise Kura::ApiError.new(r.status.errorResult.reason, r.status.errorResult.message)
        end
        return true
      end
      return false
    end

    def wait_job(job_id, timeout=60*10, project_id: @default_project_id)
      expire = Time.now + timeout
      while expire > Time.now
        j = job(job_id, project_id: project_id)
        if job_finished?(j)
          return j
        end
        if block_given?
          yield j
        end
        sleep 1
      end
      raise Kura::TimeoutError, "wait job timeout"
    end
  end
end
