# 0.1.2

* Kura::Client#load support keyword argument `file` for multipart upload.
* Fix error handling. Kura::ApiError#reason was missing.
* Add Kura::Client#list_tabledata API.

# 0.1.1

* Add Kura::Client#tables API.
* Kura::Client#wait_job yield block every second if block was passed.
