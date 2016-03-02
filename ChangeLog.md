# 0.2.11

## Enhancements

* Add Kura::Client#insert\_tabledata. This is wrapper for tabledata.insertAll.

# 0.2.10

# Fixes

* Fix NoMethodError in Kura::Client#list_tabledata when table is empty.

# 0.2.9

## Enhancements

* Add keyword argument `job_id` to query/load/extract/copy.
  You can generate unique jobId on client side and pass it to get rid of duplicated
  job insertion at network failure.
  see https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects#generate-jobid
* Add keyword argument `dry_run` to query/load/extract/copy.

## Fixes

* Add workaround of a bug in google-api-client-0.9.pre4.
  see https://github.com/google/google-api-ruby-client/issues/326

# 0.2.8

## Fixes

* BigQuery API rarely return HTML response body. Add treatment for the case.

# 0.2.7

## Fixes

* Fix job API parameter processing. The boolean parameter in nested hash could be ignored when
  `false` was passed on googla-api-client-0.9.pre3.
  The keyword arguments `flatten_results` of `query` and `print_header` of `extract` were ignored.

# 0.2.6

## Fixes

* Kura::Client#dataset and #table called with block yields nil at notFound error for consistency.

# 0.2.5

## Enhancements

* Add Kura::Client#batch and support Baches API call for #projects, #datasets, #dataset,
  #tables, #table, #list\_tabledata. The results of these api call are passwed to blocks.
  See also https://github.com/google/google-api-ruby-client#batches
  The job insertion methods (load, query, extract, copy) are not supported to call in batch's block.

# 0.2.4

## Enhancements

* Kura::ApiError contains all error messages/reasons/locaitions/deubgInfo in
  `errors` fields of response.

# 0.2.3

## Enhancements

* Support User Defined Function in Kura::Client#query.
  https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.userDefinedFunctionResources
  Kura::Client#query accept `user_defined_function_resources` keyword arguments.
  It should be a String or Array of strings.
  The string begin with "gs://" is treated as URI for GCS object.

# 0.2.2

## Enhancements

* Add Kura::Client#cancel_job. Support jobs.cancel API.
  https://cloud.google.com/bigquery/docs/reference/v2/jobs/cancel
* Kura::Client#wait_job accept Google::Apis::BigqueryV2::Job instance.
* Add Google::Apis::BigqueryV2::Job#wait and #cancel methods.

## Incompatible Changes

* Kura::Client#insert_job (and #query, #load, #extract, #copy) return job
  object (Google::Apis::BigqueryV2::Job instance) instead of job id (String).

# 0.2.1

## Enhancements

* Add Kura::Client#insert_table. Support tables.insert API.
* Add Kura::Client#patch_table. Support tables.patch API.

## Fixes

* Kura::Client#patch_dataset is now able to reset fiendly_name, description,
  default_expiration_ms by nil.

# 0.2.0

* Use google-api-client-0.9.3.pre3.

# 0.1.5

## Enhancements

* Support :http_options to pass Faraday connection.
  And set :open_timeout to 60 as default settings.

## Fixes

* Get rid of hang up at HTTP connection by set :open_timeout.

# 0.1.4

## Enhancements

* Support authentication with GCE bigquery scope.

# 0.1.3

## Enhancements

* Add Kura::Client#projects support projects.list API.
* All APIs accept `project_id` keyword argument to override @default_project_id.
* Get @default_project_id by projects.list API if not specified by the argument.

# 0.1.2

## Incompatible Changes

* Kura::Client#load: 3rd argument `source_uris` is now optional (default value is nil)
  because is is not required on multipart upload. `source_uris` and keyworkd argument `file` is exclusive.
* Kura::Client#load: 4th argument `schema` become keyword argument. It is marked as [Optional] in
  [API Reference](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schema)
* Kura::Client#query: 1st and 2nd argument `dataset_id`, `table_id` become keyord argument.
  destinationTable is an [Optional] parameter.

## Enhancements

* Kura::Client#load support keyword argument `file` for multipart upload.
* Add optional keyword arguments of Kura::Client#load.
  * `field_delimiter`
  * `allow_jagged_rows`
  * `max_bad_records`
  * `ignore_unknown_values`
  * `allow_quoted_newlines`
  * `quote`
  * `skip_leading_rows`
  * `source_format`
* Kura::Client#load keyword argument `delimiter` is deprecated. It is alias of `field_delimiter` now.
* Add optional keyword arguments of Kura::Client#query.
  * `flatten_results`
  * `priority`
  * `use_query_cache`
* Kura::Client#query keyword argument `allow_large_result` is deprecated. It is alias of `allow_large_results` now.
* Add optional keyword arguments of Kura::Client#extract.
  * `compression`
  * `destination_format`
  * `field_delimiter`
  * `print_header`
* Fix error handling. Kura::ApiError#reason was missing.
* Add Kura::Client#list_tabledata API.

# 0.1.1

## Enhancements

* Add Kura::Client#tables API.
* Kura::Client#wait_job yield block every second if block was passed.
