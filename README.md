# Kura

Kura is an interface to the [BigQuery API v2](https://cloud.google.com/bigquery/docs/reference/v2/).
This is a wrapper of google-api-client.gem.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kura'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install kura

## Usage

### Initialize

```
# a. With Service Account Private Key
client = Kura.client(project_id, email, private_key)

# b. With GCE bigquery scope (Only available on Google Compute Engine instance)
client = Kura.client
```

### Job API

```
client.load("dataset", "table", "gs://mybucket/data.csv", wait: 120)
client.query("SELECT * FROM [dataset.table];", dataset_id: "dest_dataset", table_id: dest_table", wait: 120)
client.extract("dataset", "result", "gs://mybucket/extracted.csv", wait: 120)
client.copy("src_dataset", "src_table", "dest_dataset", "dest_table", wait: 120)
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/nagachika/kura.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

