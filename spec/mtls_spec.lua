local client = require "resty.kafka.client"
local producer = require "resty.kafka.producer"

local broker_list_mtls = {
    { host = "broker", port = 29093 },
}
local client_config_mtls = {
    ssl = true,
    client_cert = CERT,
    client_priv_key = PRIV_KEY
}

describe("Testing mtls client", function()

  before_each(function()
      cli = client:new(broker_list_mtls, client_config_mtls)
      create_topics()
    end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, client_config_mtls.ssl)
    assert.are.equal(cli.socket_config.ssl_verify, false)
    assert.are.equal(cli.socket_config.client_cert, client_config_mtls.client_cert)
    assert.are.equal(cli.socket_config.client_priv_key, client_config_mtls.client_priv_key)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 29093}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 29093}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    local p, err = producer:new(broker_list_mtls, client_config_mtls)
    assert.is_nil(err)
    local offset, err = p:send("test", KEY, MESSAGE)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)

end)