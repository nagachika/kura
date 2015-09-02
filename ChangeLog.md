# 0.2.1

## Enhancements

* Add Kura::Client#insert_table. Support tables.insert API.

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
