local ssl = require("ngx.ssl")
local producer = require "resty.kafka.producer"
local request = require "resty.kafka.request"
local response = require "resty.kafka.response"

local broker_list_plain = {
	{ host = "broker", port = 9092 },
}

-- Load certificate
local f = assert(io.open("/certs/certchain.crt"))
local cert_data = f:read("*a")
f:close()
local cert, _ = ssl.parse_pem_cert(cert_data)

-- Load private key
local f = assert(io.open("/certs/privkey.key"))
local key_data = f:read("*a")
f:close()
local priv_key, _ = ssl.parse_pem_priv_key(key_data)

-- move to fixture dir or helper file
function convert_to_hex(req)
    local str = req._req[#req._req]
    local ret = ""
    for i = 1, #str do
        ret = ret .. bit.tohex(string.byte(str, i), 2)
    end
    return ret
end

-- define topics, keys and messages etc.
TEST_TOPIC = "test"
TEST_TOPIC_1 = "test1"
KEY = "key"
MESSAGE = "message"
CERT = cert
PRIV_KEY = priv_key
BROKER_LIST = broker_list_plain

function compare(func, number)
    local req = request:new(request.ProduceRequest, 1, "clientid")
    req:int32(100)
    local correlation_id = req._req[#req._req]

    req[func](req, number)
    local str = correlation_id .. req._req[#req._req]

    local resp = response:new(str, req.api_version)

    local cnumber = resp[func](resp)
    return tostring(number), number == cnumber
end


-- Create topics before running the tests
function create_topics()
    -- Not interested in the output
    local p = producer:new(broker_list_plain)
    for i=1,1000 do
        p:send(TEST_TOPIC, KEY, MESSAGE)
        p:send(TEST_TOPIC_1, KEY, MESSAGE)
    end
end

return {
	convert_to_hex = convert_to_hex,
	compare = compare
}