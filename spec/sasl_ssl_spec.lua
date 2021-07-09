local client = require "resty.kafka.client"
local producer = require "resty.kafka.producer"
local key = KEY
local message = MESSAGE

local broker_list_sasl_ssl = {
    { host = "broker", port = 9093 },
}
local sasl_config = { mechanism="PLAIN",
                      user="admin",
                      password="admin-secret" }
local client_config_sasl_ssl = {
    ssl = true,
    client_cert = CERT,
    client_priv_key = PRIV_KEY,
    auth_config = sasl_config
}

describe("Testing sasl ssl client", function()

  before_each(function()
      cli = client:new(broker_list_sasl_ssl, client_config_sasl_ssl)
  end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, client_config_sasl_ssl.ssl)
    assert.are.equal(cli.socket_config.ssl_verify, false)
    assert.are.equal(cli.auth_config.mechanism, sasl_config.mechanism)
    assert.are.equal(cli.auth_config.user, sasl_config.user)
    assert.are.equal(cli.auth_config.password, sasl_config.password)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 9093}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 9093}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    key = "foo"
    message = "bar"
    local p, err = producer:new(broker_list_sasl_ssl, client_config_sasl_ssl)
    assert.is_nil(err)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)
end)
