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
  end

  def test_dataset
    @name = "Kura_test"

    dataset = @client.dataset(@name)
    assert_nil(dataset)

    @client.insert_dataset(@name)
    dataset = @client.dataset(@name)
    assert_equal(@project_id, dataset.datasetReference.projectId)
    assert_equal(@name, dataset.datasetReference.datasetId)

    assert(@client.datasets.map(&:id).include?("#{@project_id}:#{@name}"))

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
end if File.readable?(ServiceAccountFilesPath)
