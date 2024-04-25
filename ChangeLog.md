# 1.0.2

## Changes

* Handle SystemCallError and OpenSSL::SSL::SSLError as Kura::ApiError.

# 1.0.1

## Enhancements

* `insert_table` method accept `clustering_fields` argument.

# 1.0.0

## Breaking Changes

* The default value of `use_legacy_sql` is now turn to false.

## Enhancements

* Now load/query/extract/copy methods accept keyword rest argument and pass the options to the JobConfiguration properties.

# 0.6.3

## Changes

* Add `convert_numeric_to_float` keyword argument in `list_tabledata` method.
  If `convert_numeric_to_float: true` is specified, the value from a NUMMERIC column will be converted to Float.
  The default value for `convert_numeric_to_float` is true.

# 0.6.2

## Enhancements

* Support ruby 3.0.
* Support range partitioning and time partitioning parameter for load job configuration.

# 0.6.1

## Enhancements

* `job` method now accept `fields` keyword argument.

# 0.6.0

## Changes

* Replace runtime dependency "google-api-client.gem" -> "google-apis-bigquery_v2".
  See https://github.com/groovenauts/gcs-ruby/pull/2://github.com/googleapis/google-api-ruby-client/blob/master/google-api-client/OVERVIEW.md for more details.

# 0.5.0

## Changes

* `Kura::Client#list_tabledata` now return TIMESTAMP value in ISO 8601 format String.

## Enhancements

* Accept description field in schema specification at insert&load table.

# 0.4.4

## Enhancements

* Support [Routines API](https://cloud.google.com/bigquery/docs/reference/rest/v2/routines/).

# 0.4.3

## Fixes

* Query job with SCRIPT type could contain `status.errorResult` without `status.errors` property.
  Fix to handle this case properly.

# 0.4.2

## Enhancements

* Add `jobs` to wrap `jobs.list` API method (https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list)
* Add `Job#children` as a shortcut method to obtain child jobs for SCRIPT type job.

# 0.4.1

## Fixes

* Fix NoMethodError when table schema fields from existing table for `load` and `insert_table`.

# 0.4.0

## Enhancements

* Handle table whose schema without 'mode' field. The tables created by New BigQuery Web Console could have such malformed schema.
  Thanks for GCPUG Slack for notice this issue.
* Add location: parameter for `job`/`cancel_job`/`wait_job` methods.
* `insert_dataset` can now create dataset with attributes like `location`.

# 0.3.0

## Enhancements

* Support [Models API](https://cloud.google.com/bigquery/docs/reference/rest/v2/models) to manipulate BigQuery ML models.

## Changes

* Update version dependency of google-api-client.gem to support BigQuery Models API.

# 0.2.33

## Fixes

* Fix `list_tabledata` to handle "Infinity" and "NaN" value in FLOAT columns.

# 0.2.32

## Enhancements

* Support `external_data_configuration` option.
* Add `scope` keyword argument to Kura::Client.new.

# 0.2.31

## Enhancements

* Add `autodetect` keyword argument to `load`.
* Support google-api-client.gem v0.16.x and v0.17.x series.

# 0.2.30

## Enhancements

* Support google-api-client.gem v0.15.x series (fix commit miss).

# 0.2.29

## Enhancements

* Support google-api-client.gem v0.13.x, v0.14.x, v0.15.x series.

# 0.2.28

## Enhancements

* Support google-api-client.gem v0.11.x series.

# 0.2.27

## Fixes

* Fix NoMethodError in `cancel_job` inside batch request.

# 0.2.26

## Changes

* Relax version dependency of google-api-client.gem. kura works fine with 0.10.x series.

# 0.2.25

## Enhancements

* `Kura::Client#list_tabledata` now support REPEATED/RECORD field and convert data into appropriate type of object.

# 0.2.24

## Enhancements

* Add keyword argument `page_token` to `tables`.

# 0.2.23

## Enhancements

* Support "Partitioned Tables". Add `time_partitioning` kwarg of `insert_table`.
  It seems that you cannot change partitioning settings of existing table.

# 0.2.22

## Enhancements

* Support DML queries.
  You shold pass `mode: nil` and `allow_large_results: false` explicitly to Kura::Client#query.

# 0.2.21

## Incompatible Changes

* update dependency to google-api-client.gem (>~ 0.9.11).

## Enhancements

* Support `maximum_billing_tier` and `maximum_bytes_billed` keyword arguments for `query`.

# 0.2.20

## Incompatible Changes

* `datasts` and `tables` now return `[]` instead of nil if there's no entry.

# 0.2.19

## Fixes

* `list_tabledata` can now handle REPEATED fields.

# 0.2.18

## Fixes

* Handler error media load job in batch mode.

## Enhancements

* Add keyword argument `use_legacy_sql` to Kura::Client#insert_table to
  support create View with Standard SQL.

# 0.2.17

## Incompatible Changes

* Update dependent google-api-client ~> 0.9.3.

## Enhancements

* Support `use_legacy_sql` parameter in Kura::Client#query.

# 0.2.16

## Fixes

* Fix NameError at #copy job method.
  The patch is provided by @hakobera.

# 0.2.15

## Fixes

* Fix NoMethodError if #get_job/#cancel_job return nil for job.

# 0.2.14

## Enhancements

* Kura.client now accept JSON key file.

# 0.2.13

## Enhancements

* Support #job, #cancel_job API in batch requests.

# 0.2.12

## Enhancements

* Support job APIs in batch requests.
  Until now load/query/extract/copy methods received block to be yielded when
  the keyword argument :wait was specified and polling every second.
  In batch request (in block of Kura::Client#batch), you cannot specified :wait
  keyword argument and block will be yielded when batch response are passed.

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

* Add Kura::Client#batch and support Batches API call for #projects, #datasets, #dataset,
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
  because is is not required on multipart upload. `source_uris` and keyword argument `file` is exclusive.
* Kura::Client#load: 4th argument `schema` become keyword argument. It is marked as [Optional] in
  [API Reference](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schema)
* Kura::Client#query: 1st and 2nd argument `dataset_id`, `table_id` become keyword argument.
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
