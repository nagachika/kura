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
  end

  def test_table
    assert_nil(@client.table("_dummy", "nonexist"))
    @client.table("samples", "github_timeline", project_id: "publicdata").tap do |tbl|
      assert_equal({"projectId"=>"publicdata", "datasetId"=>"samples", "tableId"=>"github_timeline"}, tbl.tableReference.to_hash)
      assert_equal("TABLE", tbl.type)
    end
  end

  def test_delete_table
    assert_nil(@client.delete_table("_dummy", "nonexist"))
    assert_raise(Kura::ApiError) do
      @client.delete_table("invalid:dataset", "nonexist")
    end
  end
end if File.readable?(ServiceAccountFilesPath)
