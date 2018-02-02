-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"


local to_int32 = response.to_int32
local setmetatable = setmetatable
local rshift = bit.rshift
local band = bit.band
local char = string.char
local ngx_log = ngx.log
local tcp = ngx.socket.tcp
local pid = ngx.worker.pid


local _M = {}
local mt = { __index = _M }


local function _send_receive(sock, payload)
    local bytes, err = sock:send(payload)
    if not bytes then
        return nil, err, true
    end

    local data, err = sock:receive(4)
    if not data then
        if err == "timeout" then
            sock:close()
            return nil, err
        end
        return nil, err, true
    end

    local len = to_int32(data)

    local data, err = sock:receive(len)
    if not data then
        if err == "timeout" then
            sock:close()
            return nil, err
        end
        return nil, err, true
    end

    if data == "" then -- send authBytes has no response
        return true
    else
        return response:new(data), nil, true
    end
end


function _M.new(self, host, port, socket_config)
    return setmetatable({
        host = host,
        port = port,
        config = socket_config,
    }, mt)
end


local function sasl_plain_handshake(sock)
    local id = 0    -- hard code correlation_id
    local client_id = "worker" .. pid()
    local req = request:new(request.SaslHandshakeRequest, id, client_id)

    req:string("PLAIN")

    local resp, err, retryable = _send_receive(sock, req:package())
    if resp then
        if resp:int16() == 0 and resp:int32() == 1 and resp:string() == "PLAIN" then
            return true
        else
            return nil, "sasl_plain not available"
        end
    else
        ngx_log(ngx.ERR, "sasl plain handshake failed!")
        return nil, err
    end
end


-- copy from file request.lua
local function str_int32(int)
    -- ngx.say(debug.traceback())
    return char(band(rshift(int, 24), 0xff),
                band(rshift(int, 16), 0xff),
                band(rshift(int, 8), 0xff),
                band(int, 0xff))
end


local function sasl_plain_auth(sock, config)
    local ok, err = sasl_plain_handshake(sock)
    if not ok then
        return nil, err
    end

    -- https://kafka.apache.org/protocol.html#sasl_handshake
    -- credentials are sent as 'opaque packets'
    -- so it should be bytes primitive uses an int32.
    -- https://github.com/Shopify/sarama/blob/0f4f8caef994ca7e4f9072c1858b7c6761ed498f/broker.go#L667
    local password
    if type(config.password) == "function" then
        password = config.password()
    else
        password = config.password
    end
    local length = 1 + #config.username + 1 + #password
    local payload = table.concat({'\0', config.username, '\0', password})
    local authBytes = {str_int32(length), payload}

    -- if auth failed, kafka server will close socket
    -- return nil, closed
    return _send_receive(sock, authBytes)
end


function _M.send_receive(self, req)
    local sock, err = tcp()
    if not sock then
        return nil, err, true
    end
    sock:settimeout(self.config.socket_timeout)

    local ok, err = sock:connect(self.host, self.port)
    if not ok then
        return nil, err, true
    end

    if sock:getreusedtimes() == 0 and self.config.sasl_enable then
        local ok, err = sasl_plain_auth(sock, self.config)
        if not ok then
            return nil, err
        end
    end

    local resp, err, retryable = _send_receive(sock, req:package())

    sock:setkeepalive(self.config.keepalive_timeout, self.config.keepalive_size)

    return resp, err, retryable
end


return _M
