# 0.1.2

## Incompatible Changes

* Kura::Client#load: 3rd argument `source_uris` is now optional (default value is nil)
  because is is not required on multipart upload. `source_uris` and keyworkd argument `file` is exclusive.
* Kura::Client#load: 4th argument `schema` become keyword argument. It is marked as [Optional] in
  [API Reference](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schema)

## Enhancements

* Kura::Client#load support keyword argument `file` for multipart upload.
* Fix error handling. Kura::ApiError#reason was missing.
* Add Kura::Client#list_tabledata API.

# 0.1.1

## Enhancements

* Add Kura::Client#tables API.
* Kura::Client#wait_job yield block every second if block was passed.
