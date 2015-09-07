# coding: utf-8

require "google/apis/bigquery_v2"
require "googleauth"
require "kura/version"

module Kura
  class Client
    def initialize(default_project_id: nil, email_address: nil, private_key: nil, http_options: {timeout: 60}, default_retries: 5)
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
        # MEMO: signet-0.6.1 depend on Farady.default_connection
        Faraday.default_connection.options.timeout = 60
        auth.fetch_access_token!
      else
        auth = Google::Auth.get_application_default([@scope])
        auth.fetch_access_token!
      end
      Google::Apis::RequestOptions.default.retries = default_retries
      Google::Apis::RequestOptions.default.timeout_sec = http_options[:timeout]
      @api = Google::Apis::BigqueryV2::BigqueryService.new
      @api.authorization = auth

      if @default_project_id.nil?
        @default_project_id = self.projects.first.id
      end
    end

    def process_error(err)
      if err.respond_to?(:body)
        jobj = JSON.parse(err.body)
        error = jobj["error"]
        error = error["errors"][0]
        raise Kura::ApiError.new(error["reason"], error["message"])
      else
        raise err
      end
    end
    private :process_error

    def projects(limit: 1000)
      result = @api.list_projects(max_results: limit)
      result.projects
    rescue
      process_error($!)
    end

    def datasets(project_id: @default_project_id, all: false, limit: 1000)
      result = @api.list_datasets(project_id, all: all, max_results: limit)
      result.datasets
    rescue
      process_error($!)
    end

    def dataset(dataset_id, project_id: @default_project_id)
      @api.get_dataset(project_id, dataset_id)
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def insert_dataset(dataset_id, project_id: @default_project_id)
      obj = Google::Apis::BigqueryV2::Dataset.new(dataset_reference: Google::Apis::BigqueryV2::DatasetReference.new(project_id: project_id, dataset_id: dataset_id))
      @api.insert_dataset(project_id, obj)
    rescue
      process_error($!)
    end

    def delete_dataset(dataset_id, project_id: @default_project_id, delete_contents: false)
      @api.delete_dataset(project_id, dataset_id, delete_contents: delete_contents)
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def patch_dataset(dataset_id, project_id: @default_project_id, access: nil, description: :na, default_table_expiration_ms: :na, friendly_name: :na )
      obj = Google::Apis::BigqueryV2::Dataset.new(dataset_reference: Google::Apis::BigqueryV2::DatasetReference.new(project_id: project_id, dataset_id: dataset_id))
      obj.access = access if access
      obj.default_table_expiration_ms = default_table_expiration_ms if default_table_expiration_ms != :na
      obj.description = description if description != :na
      obj.friendly_name = friendly_name if friendly_name != :na
      @api.patch_dataset(project_id, dataset_id, obj)
    rescue
      process_error($!)
    end

    def tables(dataset_id, project_id: @default_project_id, limit: 1000)
      result = @api.list_tables(project_id, dataset_id, max_results: limit)
      result.tables
    rescue
      process_error($!)
    end

    def table(dataset_id, table_id, project_id: @default_project_id)
      @api.get_table(project_id, dataset_id, table_id)
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def insert_table(dataset_id, table_id, project_id: @default_project_id, expiration_time: nil,
                     friendly_name: nil, schema: nil, description: nil,
                     query: nil, external_data_configuration: nil)
      if expiration_time
        expiration_time = (expiration_time.to_f * 1000.0).to_i
      end
      if query
        view = { query: query }
      elsif external_data_configuration
      elsif schema
        schema = { fields: normalize_schema(schema) }
      end
      table = Google::Apis::BigqueryV2::Table.new(
        table_reference: {project_id: project_id, dataset_id: dataset_id, table_id: table_id},
        friendly_name: friendly_name,
        description: description,
        schema: schema,
        expiration_time: expiration_time,
        view: view,
        external_data_configuration: external_data_configuration)
      @api.insert_table(project_id, dataset_id, table)
    rescue
      process_error($!)
    end

    def patch_table(dataset_id, table_id, project_id: @default_project_id, expiration_time: :na, friendly_name: :na, description: :na)
      if expiration_time != :na and not(expiration_time.nil?)
        expiration_time = (expiration_time.to_f * 1000.0).to_i
      end
      table = Google::Apis::BigqueryV2::Table.new(table_reference: {project_id: project_id, dataset_id: dataset_id, table_id: table_id})
      table.friendly_name = friendly_name if friendly_name != :na
      table.description = description if description != :na
      table.expiration_time = expiration_time if expiration_time != :na
      @api.patch_table(project_id, dataset_id, table_id, table)
    rescue
      process_error($!)
    end

    def delete_table(dataset_id, table_id, project_id: @default_project_id)
      @api.delete_table(project_id, dataset_id, table_id)
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def list_tabledata(dataset_id, table_id, project_id: @default_project_id, start_index: 0, max_result: 100, page_token: nil, schema: nil)
      schema ||= table(dataset_id, table_id, project_id: project_id).schema.fields
      field_names = schema.map{|f| f.respond_to?(:[]) ? (f["name"] || f[:name]) : f.name }

      r = @api.list_table_data(project_id, dataset_id, table_id, max_results: max_result, start_index: start_index, page_token: page_token)
      {
        total_rows: r.total_rows.to_i,
        next_token: r.page_token,
        rows: r.rows.map do |row|
          row.f.zip(field_names).each_with_object({}) do |(v, fn), tbl| tbl[fn] = v.v end
        end
      }
    rescue
      process_error($!)
    end

    def mode_to_write_disposition(mode)
      unless %i{ append truncate empty }.include?(mode)
        raise "mode option should be one of :append, :truncate, :empty but #{mode}"
      end
      "WRITE_#{mode.to_s.upcase}"
    end
    private :mode_to_write_disposition

    def insert_job(configuration, project_id: @default_project_id, media: nil, wait: nil)
      job_object = Google::Apis::BigqueryV2::Job.new
      job_object.configuration = configuration
      result = @api.insert_job(project_id, job_object, upload_source: media)
      job_id = result.job_reference.job_id
      if wait
        wait_job(job_id, wait, project_id: project_id)
      else
        job_id
      end
    rescue
      process_error($!)
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
          write_disposition: write_disposition,
          allow_large_results: allow_large_results,
          flatten_results: flatten_results,
          priority: priority,
          use_query_cache: use_query_cache,
        }
      }
      if dataset_id and table_id
        configuration[:query][:destination_table] = { project_id: project_id, dataset_id: dataset_id, table_id: table_id }
      end
      insert_job(configuration, wait: wait, project_id: job_project_id)
    end

    def normalize_schema(schema)
      schema.map do |s|
        if s.respond_to?(:[])
          f = {
            name: (s[:name] || s["name"]),
            type: (s[:type] || s["type"]),
            mode: (s[:mode] || s["mode"]),
          }
          if (sub_fields = (s[:fields] || s["fields"]))
            f[:fields] = normalize_schema(sub_fields)
          end
        else
          f = {
            name: s.name,
            type: s.type,
            mode: s.mode,
          }
          if (sub_fields = f.fields)
            f[:fields] = normalize_schema(sub_fields)
          end
        end
        f
      end
    end
    private :normalize_schema

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
          destination_table: {
            project_id: project_id,
            dataset_id: dataset_id,
            table_id: table_id,
          },
          write_disposition: write_disposition,
          allow_jagged_rows: allow_jagged_rows,
          max_bad_records: max_bad_records,
          ignore_unknown_values: ignore_unknown_values,
          source_format: source_format,
        }
      }
      if schema
        configuration[:load][:schema] = { fields: normalize_schema(schema) }
      end
      if source_format == "CSV"
        configuration[:load][:field_delimiter] = field_delimiter
        configuration[:load][:allow_quoted_newlines] = allow_quoted_newlines
        configuration[:load][:quote] = quote
        configuration[:load][:skip_leading_rows] = skip_leading_rows
      end
      unless file
        configuration[:load][:source_uris] = source_uris
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
          destination_format: destination_format,
          source_table: {
            project_id: project_id,
            dataset_id: dataset_id,
            table_id: table_id,
          },
          destination_uris: dest_uris,
        }
      }
      if destination_format == "CSV"
        configuration[:extract][:field_delimiter] = field_delimiter
        configuration[:extract][:print_header] = print_header
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
          destination_table: {
            project_id: dest_project_id,
            dataset_id: dest_dataset_id,
            table_id: dest_table_id,
          },
          source_table: {
            project_id: src_project_id,
            dataset_id: src_dataset_id,
            table_id: src_table_id,
          },
          write_disposition: write_disposition,
        }
      }
      insert_job(configuration, wait: wait, project_id: job_project_id)
    end

    def job(job_id, project_id: @default_project_id)
      @api.get_job(project_id, job_id)
    rescue
      process_error($!)
    end

    def cancel_job(job, project_id: @default_project_id)
      case job
      when String
        jobid = job
      when Google::Apis::BigqueryV2::Job
        project_id = job.job_reference.project_id
        jobid = job.job_reference.job_id
      end
      @api.cancel_job(project_id, jobid).job
    end

    def job_finished?(r)
      if r.status.state == "DONE"
        if r.status.error_result
          raise Kura::ApiError.new(r.status.error_result.reason, r.status.error_result.message)
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
