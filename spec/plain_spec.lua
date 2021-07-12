local client = require "resty.kafka.client"
local producer = require "resty.kafka.producer"

local key = KEY
local message = MESSAGE

local broker_list_plain = {
	{ host = "broker", port = 9092 },
}
  local broker_list_plain_bad_broker = {
        { host = "broker", port = 9999 },
        { host = "broker", port = 9092 }
  }
describe("Testing plain client", function()

  before_each(function()
      cli = client:new(broker_list_plain)
      create_topics()
    end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, false)
    assert.are.equal(cli.socket_config.ssl_verify, false)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 9092}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 9092}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.is_not_nil(cli.supported_api_versions)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    local p, err = producer:new(broker_list_plain)
    assert.is_nil(err)
    local offset, err = p:send(TEST_TOPIC, KEY, MESSAGE)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)
end)

describe("Testing plain client with a bad broker in the bootstrap list", function()

  before_each(function()
      cli = client:new(broker_list_plain_bad_broker)
      create_topics()
    end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, false)
    assert.are.equal(cli.socket_config.ssl_verify, false)
  end)

  it("to fetch metadata correctly and exclude the bad broker", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    -- Expect the bad broker to not appear in this list
    assert.are.same({{host = "broker", port = 9092}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 9092}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    local p, err = producer:new(broker_list_plain_bad_broker)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)

end)

