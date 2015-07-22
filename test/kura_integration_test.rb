require 'test_helper'
require "json"

ServiceAccountFilesPath = File.expand_path("../data/service_account.json", __FILE__)

class KuraIntegrationTest < Test::Unit::TestCase
  def setup
    service_account = JSON.parse(File.binread(ServiceAccountFilesPath))
    @project_id = service_account["project_id"]
    @email = service_account["client_email"]
    @private_key = service_account["private_key"]

    @client = Kura.client(@project_id, @email, @private_key)

    # for reduce power_assert display
    def @client.inspect
      super[0, 15] + "...>"
    end
  end

  def test_dataset
    @name = "Kura_test"

    dataset = @client.dataset(@name)
    assert_nil(dataset)

    @client.insert_dataset(@name)
    dataset = @client.dataset(@name)
    assert_equal(@project_id, dataset.datasetReference.projectId)
    assert_equal(@name, dataset.datasetReference.datasetId)

    power_assert do
      @client.datasets.map(&:id).include?("#{@project_id}:#{@name}")
    end

    access = @client.dataset(@name).access.map(&:to_hash)
    access << {
      "role" => "READER",
      "domain" => "example.com",
    }
    @client.patch_dataset(@name, access: access, description: "Description#1", default_table_expiration_ms: 3_600_000, friendly_name: "FriendlyName#1")
    @client.dataset(@name).tap do |d|
      assert_equal(access, d.access.map(&:to_hash))
      assert_equal("Description#1", d.description)
      assert_equal(3_600_000, d.defaultTableExpirationMs)
      assert_equal("FriendlyName#1", d.friendlyName)
    end

    @client.delete_dataset(@name)
    dataset = @client.dataset(@name)
    assert_nil(dataset)

    err = assert_raise(Kura::ApiError) { @client.datasets(project_id: "invalid-project-000") }
    assert_equal("notFound", err.reason)
    assert_match(/invalid-project-000/, err.message)

    err = assert_raise(Kura::ApiError) { @client.dataset("invalid:dataset") }
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)

    err = assert_raise(Kura::ApiError) { @client.insert_dataset("invalid:dataset") }
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)

    err = assert_raise(Kura::ApiError) { @client.delete_dataset("invalid:dataset") }
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)

    err = assert_raise(Kura::ApiError) { @client.patch_dataset("invalid:dataset", description: "dummy") }
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)
  end

  def test_tables
    @client.tables("samples", project_id: "publicdata").tap do |result|
      assert_equal(7, result.size)
      assert_equal([
        "publicdata:samples.github_nested",
        "publicdata:samples.github_timeline",
        "publicdata:samples.gsod",
        "publicdata:samples.natality",
        "publicdata:samples.shakespeare",
        "publicdata:samples.trigrams",
        "publicdata:samples.wikipedia",
      ], result.map(&:id).sort)
    end

    err = assert_raise(Kura::ApiError) do
      @client.tables("nonexist")
    end
    assert_equal("notFound", err.reason)
    assert_match(/nonexist/, err.message)
  end

  def test_table
    assert_nil(@client.table("_dummy", "nonexist"))
    @client.table("samples", "github_timeline", project_id: "publicdata").tap do |tbl|
      assert_equal({"projectId"=>"publicdata", "datasetId"=>"samples", "tableId"=>"github_timeline"}, tbl.tableReference.to_hash)
      assert_equal("TABLE", tbl.type)
    end

    err = assert_raise(Kura::ApiError) do
      @client.table("invalid:dataset", "table")
    end
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)
  end

  def test_delete_table
    assert_nil(@client.delete_table("_dummy", "nonexist"))
    err = assert_raise(Kura::ApiError) do
      @client.delete_table("invalid:dataset", "nonexist")
    end
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)
  end

  def test_list_tableata_with_invalid_dataset
    err = assert_raise(Kura::ApiError) { @client.list_tabledata("invalid:dataset", "nonexist", schema: [{"name": "f1"}]) }
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)
  end

  def test_query_with_invalid_dataset
    err = assert_raise(Kura::ApiError) { @client.query("INVALID SQL", dataset_id: "invalid:dataset", table_id: "dummy") }
    assert_equal("invalid", err.reason)
    assert_match(/invalid:dataset/, err.message)
  end

  def test_query_and_tabledata
    dataset = "_Kura_test"
    table = "Kura_query_result1"
    unless @client.dataset(dataset)
      @client.insert_dataset(dataset)
    end

    assert_nothing_raised do
      @client.query("SELECT count(*) FROM [publicdata:samples.wikipedia]", dataset_id: dataset, table_id: table, wait: 60)
    end

    assert_equal({next_token: nil, rows: [{"f0_"=>"313797035"}], total_rows: 1}, @client.list_tabledata(dataset, table))
    @client.delete_table(dataset, table)
  ensure
    @client.delete_dataset(dataset, delete_contents: true)
  end

  def test_media_upload
    dataset = "_Kura_test"
    table = "Kura_upload_test1"
    unless @client.dataset(dataset)
      @client.insert_dataset(dataset)
    end

    schema = [
      { name: "f1", type: "STRING", mode: "NULLABLE" },
      { name: "f2", type: "STRING", mode: "NULLABLE" },
    ]
    io = StringIO.new(<<-EOC.gsub(/^\s+/, ""))
      aaa,bbb
      ccc,ddd
    EOC
    assert_nothing_raised do
      job_id = @client.load(dataset, table, schema: schema, file: io, mode: :truncate)
      @client.wait_job(job_id, 300)
    end
    power_assert do
      "#{@project_id}:#{dataset}.#{table}" == @client.table(dataset, table).id
    end
    power_assert do
      @client.list_tabledata(dataset, table) == {total_rows: 2, next_token: nil, rows: [{"f1" => "aaa", "f2" => "bbb"},{"f1" => "ccc", "f2" => "ddd"}]}
    end
  ensure
    @client.delete_dataset(dataset, delete_contents: true)
  end

  default_expected = [{"f1"=>"aaa", "f2"=>"bbb"}]
  data({
    field_delimiter: [ "aaa!bbb\n", {field_delimiter: "!"}, default_expected ],
    allow_jagged_rows: [ "aaa\n", {allow_jagged_rows: true}, [{"f1"=>"aaa","f2"=>nil}] ],
    max_bad_records: [ "xxx\naaa,bbb\n", {max_bad_records: 1}, default_expected ],
    ignore_unknown_values: ["aaa,bbb,ccc\n", {ignore_unknown_values: true}, default_expected ],
    allow_quoted_newlines: [%{"aaa\naaa",bbb}, {allow_quoted_newlines: true}, [{"f1"=>"aaa\naaa", "f2"=>"bbb"}] ],
    skip_leading_rows: ["xxx,yyy\naaa,bbb\n", {skip_leading_rows: 1}, default_expected ],
    source_format: [default_expected.first.to_json+"\n", {source_format: "NEWLINE_DELIMITED_JSON"}, default_expected],
  })
  def test_load_parameters(data)
    csv, options, expected = data
    dataset = "_Kura_test"
    table = "Kura_load_parameter_#{options.keys.join("_")}_test"
    schema = [
      { name: "f1", type: "STRING", mode: "NULLABLE" },
      { name: "f2", type: "STRING", mode: "NULLABLE" },
    ]
    unless @client.dataset(dataset)
      @client.insert_dataset(dataset)
    end
    assert_nothing_raised do
      @client.load(dataset, table, schema: schema, file: StringIO.new(csv), **options, wait: 300)
    end
    power_assert do
      @client.list_tabledata(dataset, table) == {total_rows: expected.size, next_token: nil, rows: expected}
    end
  ensure
    @client.delete_dataset(dataset, delete_contents: true)
  end
end if File.readable?(ServiceAccountFilesPath)
