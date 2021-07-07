local client = require "resty.kafka.client"
local producer = require "resty.kafka.producer"
local ssl = require("ngx.ssl")

local f = assert(io.open("/certs/certchain.crt"))
local cert_data = f:read("*a")
f:close()

local CERT, err = ssl.parse_pem_cert(cert_data)

local f = assert(io.open("/certs/privkey.key"))
local key_data = f:read("*a")
f:close()

local CERT_KEY, err = ssl.parse_pem_priv_key(key_data)

local TEST_TOPIC = "test"

local broker_list_plain = {
    { host = "broker", port = 9092 },
}
local broker_list_sasl = {
    { host = "broker", port = 19093 },
}
local broker_list_mtls = {
    { host = "broker", port = 29093 },
}
local sasl_config = { mechanism="PLAIN",
                      user="admin",
                      password="admin-secret" }

local client_config_mtls = {
    ssl = true,
    client_cert = CERT,
    client_priv_key = CERT_KEY
}

local client_config_sasl_plain = {
    ssl = false,
    auth_config = sasl_config
}

describe("Testing sasl client", function()

  before_each(function()
      cli = client:new(broker_list_sasl, client_config_sasl_plain)
  end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, client_config_sasl_plain.ssl)
    assert.are.equal(cli.socket_config.ssl_verify, false)
    assert.are.equal(cli.auth_config.mechanism, sasl_config.mechanism)
    assert.are.equal(cli.auth_config.user, sasl_config.user)
    assert.are.equal(cli.auth_config.password, sasl_config.password)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 19093}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 19093}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    key = "foo"
    message = "bar"
    local p, err = producer:new(broker_list_sasl, client_config_sasl_plain)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)
end)

describe("Testing mtls client", function()

  before_each(function()
      cli = client:new(broker_list_mtls, client_config_mtls)
    end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, client_config_mtls.ssl)
    assert.are.equal(cli.socket_config.ssl_verify, false)
    assert.are.equal(cli.socket_config.client_cert, CERT)
    assert.are.equal(cli.socket_config.client_priv_key, CERT_KEY)
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
    key = "foo"
    message = "bar"
    local p, err = producer:new(broker_list_mtls, client_config_mtls)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)

end)

describe("Testing plain client", function()

  before_each(function()
      cli = client:new(broker_list_plain)
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
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    key = "foo"
    message = "bar"
    local p, err = producer:new(broker_list_plain)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)

end)