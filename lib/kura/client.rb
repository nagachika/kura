# coding: utf-8

require "json"
require "google/apis/bigquery_v2"
require "googleauth"
require "kura/version"
require "kura/extensions"

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

    def normalize_parameter(v)
      case v
      when nil
        nil
      else
        v.to_s
      end
    end

    def process_error(err)
      if err.respond_to?(:body) and err.body
        begin
          jobj = JSON.parse(err.body)
          error = jobj["error"]
          reason = error["errors"].map{|e| e["reason"]}.join(",")
          errors = error["errors"].map{|e| e["message"] }.join("\n")
        rescue JSON::ParserError
          reason = err.status_code.to_s
          errors = "HTTP Status: #{err.status_code}\nHeaders: #{err.header.inspect}\nBody:\n#{err.body}"
        end
        raise Kura::ApiError.new(reason, errors)
      else
        raise err
      end
    end
    private :process_error

    def batch
      @api.batch do |api|
        original_api, @api = @api, api
        begin
          yield
        ensure
          @api = original_api
        end
      end
    end

    def projects(limit: 1000, &blk)
      if blk
        @api.list_projects(max_results: limit) do |result, err|
          result &&= result.projects
          blk.call(result, err)
        end
      else
        result = @api.list_projects(max_results: limit)
        result.projects
      end
    rescue
      process_error($!)
    end

    def datasets(project_id: @default_project_id, all: false, limit: 1000, &blk)
      all = normalize_parameter(all)
      if blk
        @api.list_datasets(project_id, all: all, max_results: limit) do |result, err|
          result &&= result.datasets
          blk.call(result, err)
        end
      else
        result = @api.list_datasets(project_id, all: all, max_results: limit)
        result.datasets
      end
    rescue
      process_error($!)
    end

    def dataset(dataset_id, project_id: @default_project_id, &blk)
      if blk
        @api.get_dataset(project_id, dataset_id) do |result, err|
          if err.respond_to?(:status_code) and err.status_code == 404
            result = nil
            err = nil
          end
          blk.call(result, err)
        end
      else
        @api.get_dataset(project_id, dataset_id)
      end
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def insert_dataset(dataset_id, project_id: @default_project_id, &blk)
      obj = Google::Apis::BigqueryV2::Dataset.new(dataset_reference: Google::Apis::BigqueryV2::DatasetReference.new(project_id: project_id, dataset_id: dataset_id))
      @api.insert_dataset(project_id, obj, &blk)
    rescue
      process_error($!)
    end

    def delete_dataset(dataset_id, project_id: @default_project_id, delete_contents: false, &blk)
      delete_contents = normalize_parameter(delete_contents)
      @api.delete_dataset(project_id, dataset_id, delete_contents: delete_contents, &blk)
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def patch_dataset(dataset_id, project_id: @default_project_id, access: nil, description: :na, default_table_expiration_ms: :na, friendly_name: :na, &blk)
      obj = Google::Apis::BigqueryV2::Dataset.new(dataset_reference: Google::Apis::BigqueryV2::DatasetReference.new(project_id: project_id, dataset_id: dataset_id))
      obj.access = access if access
      obj.default_table_expiration_ms = default_table_expiration_ms if default_table_expiration_ms != :na
      obj.description = description if description != :na
      obj.friendly_name = friendly_name if friendly_name != :na
      @api.patch_dataset(project_id, dataset_id, obj, &blk)
    rescue
      process_error($!)
    end

    def tables(dataset_id, project_id: @default_project_id, limit: 1000, &blk)
      if blk
        @api.list_tables(project_id, dataset_id, max_results: limit) do |result, err|
          result &&= result.tables
          blk.call(result, err)
        end
      else
        result = @api.list_tables(project_id, dataset_id, max_results: limit)
        result.tables
      end
    rescue
      process_error($!)
    end

    def table(dataset_id, table_id, project_id: @default_project_id, &blk)
      if blk
        @api.get_table(project_id, dataset_id, table_id) do |result, err|
          if err.respond_to?(:status_code) and err.status_code == 404
            result = nil
            err = nil
          end
          blk.call(result, err)
        end
      else
        @api.get_table(project_id, dataset_id, table_id)
      end
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def insert_table(dataset_id, table_id, project_id: @default_project_id, expiration_time: nil,
                     friendly_name: nil, schema: nil, description: nil,
                     query: nil, external_data_configuration: nil,
                     use_legacy_sql: true, &blk)
      if expiration_time
        expiration_time = (expiration_time.to_f * 1000.0).to_i
      end
      if query
        view = { query: query, use_legacy_sql: !!use_legacy_sql }
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
      @api.insert_table(project_id, dataset_id, table, &blk)
    rescue
      process_error($!)
    end

    def patch_table(dataset_id, table_id, project_id: @default_project_id, expiration_time: :na, friendly_name: :na, description: :na, &blk)
      if expiration_time != :na and not(expiration_time.nil?)
        expiration_time = (expiration_time.to_f * 1000.0).to_i
      end
      table = Google::Apis::BigqueryV2::Table.new(table_reference: {project_id: project_id, dataset_id: dataset_id, table_id: table_id})
      table.friendly_name = friendly_name if friendly_name != :na
      table.description = description if description != :na
      table.expiration_time = expiration_time if expiration_time != :na
      @api.patch_table(project_id, dataset_id, table_id, table, &blk)
    rescue
      process_error($!)
    end

    def delete_table(dataset_id, table_id, project_id: @default_project_id, &blk)
      @api.delete_table(project_id, dataset_id, table_id, &blk)
    rescue
      return nil if $!.respond_to?(:status_code) and $!.status_code == 404
      process_error($!)
    end

    def format_tabledata(r, field_names)
      {
        total_rows: r.total_rows.to_i,
        next_token: r.page_token,
        rows: (r.rows || []).map do |row|
          row.f.zip(field_names).each_with_object({}) do |(v, fn), tbl|
            if v.v.is_a?(Array)
              tbl[fn] = v.v.map{|c| c["v"] }
            else
              tbl[fn] = v.v
            end
          end
        end
      }
    end
    private :format_tabledata

    def list_tabledata(dataset_id, table_id, project_id: @default_project_id, start_index: 0, max_result: 100, page_token: nil, schema: nil, &blk)
      schema ||= table(dataset_id, table_id, project_id: project_id).schema.fields
      field_names = schema.map{|f| f.respond_to?(:[]) ? (f["name"] || f[:name]) : f.name }

      if blk
        @api.list_table_data(project_id, dataset_id, table_id, max_results: max_result, start_index: start_index, page_token: page_token) do |r, err|
          if r
            r = format_tabledata(r, field_names)
          end
          blk.call(r, err)
        end
      else
        r = @api.list_table_data(project_id, dataset_id, table_id, max_results: max_result, start_index: start_index, page_token: page_token)
        format_tabledata(r, field_names)
      end
    rescue
      process_error($!)
    end

    def insert_tabledata(dataset_id, table_id, rows, project_id: @default_project_id, ignore_unknown_values: false, skip_invalid_rows: false, template_suffix: nil)
      request = Google::Apis::BigqueryV2::InsertAllTableDataRequest.new
      request.ignore_unknown_values = ignore_unknown_values
      request.skip_invalid_rows = skip_invalid_rows
      if template_suffix
        request.template_suffix = template_suffix
      end
      request.rows = rows.map do |r|
        case r
        when Google::Apis::BigqueryV2::InsertAllTableDataRequest::Row
          r
        when Hash
          row = Google::Apis::BigqueryV2::InsertAllTableDataRequest::Row.new
          if r.keys.map(&:to_s) == %w{ insert_id json }
            row.insert_id = r[:insert_id] || r["insert_id"]
            row.json = r[:json] || r["json"]
          else
            row.json = r
          end
          row
        else
          raise ArgumentError, "invalid row for BigQuery tabledata.insertAll #{r.inspect}"
        end
      end

      r = @api.insert_all_table_data(project_id, dataset_id, table_id, request)
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

    def insert_job(configuration, job_id: nil, project_id: @default_project_id, media: nil, wait: nil, &blk)
      job_object = Google::Apis::BigqueryV2::Job.new
      job_object.configuration = configuration
      if job_id
        job_object.job_reference = Google::Apis::BigqueryV2::JobReference.new
        job_object.job_reference.project_id = project_id
        job_object.job_reference.job_id = job_id
      end
      if wait
        job = @api.insert_job(project_id, job_object, upload_source: media)
        job.kura_api = self
        wait_job(job, wait, &blk)
      else
        if blk
          @api.insert_job(project_id, job_object, upload_source: media) do |r, err|
            if r
              r.kura_api = self
            end
            blk.call(r, err)
          end
        else
          job = @api.insert_job(project_id, job_object, upload_source: media)
          job.kura_api = self
          job
        end
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
              user_defined_function_resources: nil,
              use_legacy_sql: true,
              project_id: @default_project_id,
              job_project_id: @default_project_id,
              job_id: nil,
              wait: nil,
              dry_run: false,
              &blk)
      write_disposition = mode_to_write_disposition(mode)
      configuration = Google::Apis::BigqueryV2::JobConfiguration.new({
        query: Google::Apis::BigqueryV2::JobConfigurationQuery.new({
          query: sql,
          write_disposition: write_disposition,
          allow_large_results: normalize_parameter(allow_large_results),
          flatten_results: normalize_parameter(flatten_results),
          priority: priority,
          use_query_cache: normalize_parameter(use_query_cache),
          use_legacy_sql: use_legacy_sql,
        })
      })
      if dry_run
        configuration.dry_run = true
        wait = nil
      end
      if dataset_id and table_id
        configuration.query.destination_table = Google::Apis::BigqueryV2::TableReference.new({ project_id: project_id, dataset_id: dataset_id, table_id: table_id })
      end
      if user_defined_function_resources
        configuration.query.user_defined_function_resources = Array(user_defined_function_resources).map do |r|
          r = r.to_s
          if r.start_with?("gs://")
            Google::Apis::BigqueryV2::UserDefinedFunctionResource.new({ resource_uri: r })
          else
            Google::Apis::BigqueryV2::UserDefinedFunctionResource.new({ inline_code: r })
          end
        end
      end
      insert_job(configuration, wait: wait, job_id: job_id, project_id: job_project_id, &blk)
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
             job_id: nil,
             file: nil, wait: nil,
             dry_run: false,
             &blk)
      write_disposition = mode_to_write_disposition(mode)
      source_uris = [source_uris] if source_uris.is_a?(String)
      configuration = Google::Apis::BigqueryV2::JobConfiguration.new({
        load: Google::Apis::BigqueryV2::JobConfigurationLoad.new({
          destination_table: Google::Apis::BigqueryV2::TableReference.new({
            project_id: project_id,
            dataset_id: dataset_id,
            table_id: table_id,
          }),
          write_disposition: write_disposition,
          allow_jagged_rows: normalize_parameter(allow_jagged_rows),
          max_bad_records: max_bad_records,
          ignore_unknown_values: normalize_parameter(ignore_unknown_values),
          source_format: source_format,
        })
      })
      if dry_run
        configuration.dry_run = true
        wait = nil
      end
      if schema
        configuration.load.schema = Google::Apis::BigqueryV2::TableSchema.new({ fields: normalize_schema(schema) })
      end
      if source_format == "CSV"
        configuration.load.field_delimiter = field_delimiter
        configuration.load.allow_quoted_newlines = normalize_parameter(allow_quoted_newlines)
        configuration.load.quote = quote
        configuration.load.skip_leading_rows = skip_leading_rows
      end
      unless file
        configuration.load.source_uris = source_uris
      end
      insert_job(configuration, media: file, wait: wait, job_id: job_id, project_id: job_project_id, &blk)
    end

    def extract(dataset_id, table_id, dest_uris,
                compression: "NONE",
                destination_format: "CSV",
                field_delimiter: ",",
                print_header: true,
                project_id: @default_project_id,
                job_project_id: @default_project_id,
                job_id: nil,
                wait: nil,
                dry_run: false,
                &blk)
      dest_uris = [ dest_uris ] if dest_uris.is_a?(String)
      configuration = Google::Apis::BigqueryV2::JobConfiguration.new({
        extract: Google::Apis::BigqueryV2::JobConfigurationExtract.new({
          compression: compression,
          destination_format: destination_format,
          source_table: Google::Apis::BigqueryV2::TableReference.new({
            project_id: project_id,
            dataset_id: dataset_id,
            table_id: table_id,
          }),
          destination_uris: dest_uris,
        })
      })
      if dry_run
        configuration.dry_run = true
        wait = nil
      end
      if destination_format == "CSV"
        configuration.extract.field_delimiter = field_delimiter
        configuration.extract.print_header = normalize_parameter(print_header)
      end
      insert_job(configuration, wait: wait, job_id: job_id, project_id: job_project_id, &blk)
    end

    def copy(src_dataset_id, src_table_id, dest_dataset_id, dest_table_id,
             mode: :truncate,
             src_project_id: @default_project_id,
             dest_project_id: @default_project_id,
             job_project_id: @default_project_id,
             job_id: nil,
             wait: nil,
             dry_run: false,
             &blk)
      write_disposition = mode_to_write_disposition(mode)
      configuration = Google::Apis::BigqueryV2::JobConfiguration.new({
        copy: Google::Apis::BigqueryV2::JobConfigurationTableCopy.new({
          destination_table: Google::Apis::BigqueryV2::TableReference.new({
            project_id: dest_project_id,
            dataset_id: dest_dataset_id,
            table_id: dest_table_id,
          }),
          source_table: Google::Apis::BigqueryV2::TableReference.new({
            project_id: src_project_id,
            dataset_id: src_dataset_id,
            table_id: src_table_id,
          }),
          write_disposition: write_disposition,
        })
      })
      if dry_run
        configuration.dry_run = true
        wait = nil
      end
      insert_job(configuration, wait: wait, job_id: job_id, project_id: job_project_id, &blk)
    end

    def job(job_id, project_id: @default_project_id, &blk)
      if blk
        @api.get_job(project_id, job_id) do |j, e|
          j.kura_api = self if j
          blk.call(j, e)
        end
      else
        @api.get_job(project_id, job_id).tap{|j| j.kura_api = self if j }
      end
    rescue
      process_error($!)
    end

    def cancel_job(job, project_id: @default_project_id, &blk)
      case job
      when String
        jobid = job
      when Google::Apis::BigqueryV2::Job
        project_id = job.job_reference.project_id
        jobid = job.job_reference.job_id
      else
        raise TypeError, "Kura::Client#cancel_job accept String(job-id) or Google::Apis::BigqueryV2::Job"
      end
      if blk
        @api.cancel_job(project_id, jobid) do |r, e|
          r.job.kura_api = self if r.job
          blk.call(r.job, e)
        end
      else
        @api.cancel_job(project_id, jobid).job.tap{|j| j.kura_api = self if j }
      end
    end

    def job_finished?(r)
      if r.status.state == "DONE"
        if r.status.error_result
          raise Kura::ApiError.new(r.status.errors.map(&:reason).join(","),
                                   r.status.errors.map{|e|
                                     msg = "reason=#{e.reason} message=#{e.message}"
                                     msg += " location=#{e.location}" if e.location
                                     msg += " debug_infoo=#{e.debug_info}" if e.debug_info
                                     msg
                                   }.join("\n"))
        end
        return true
      end
      return false
    end

    def wait_job(job, timeout=60*10, project_id: @default_project_id)
      case job
      when String
        job_id = job
      when Google::Apis::BigqueryV2::Job
        project_id = job.job_reference.project_id
        job_id = job.job_reference.job_id
      else
        raise TypeError, "Kura::Client#wait_job accept String(job-id) or Google::Apis::BigqueryV2::Job"
      end
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
