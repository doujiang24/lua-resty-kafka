-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local auth_utils = require "resty.kafka.auth.init"

local to_int32 = response.to_int32
local setmetatable = setmetatable
local tcp = ngx.socket.tcp


local _M = {}
local mt = { __index = _M }


function _M.new(self, host, port, socket_config, auth_config)
    return setmetatable({
        host = host,
        port = port,
        config = socket_config,
        auth = auth_config or nil,
    }, mt)
end

function _M.send_receive(self, request)
    local sock, err = tcp()
    if not sock then
        return nil, err, true
    end

    sock:settimeout(self.config.socket_timeout)

    local ok, err = sock:connect(self.host, self.port)
    if not ok then
        return nil, err, true
    end
    if self.config.ssl then
        -- TODO: add reused_session for better performance of short-lived connections
        local opts = {
            ssl_verify = self.config.ssl_verify,
            client_cert = self.config.client_cert,
            client_priv_key = self.config.client_priv_key,
        }
        
        -- TODO END
        local _, err = sock:tlshandshake(opts)
        if err then
            ngx.say(err)
            return nil, "failed to do SSL handshake with " ..
                        self.host .. ":" .. tostring(self.port) .. ": " .. err, true
        end
    end

    -- authenticate if auth config is passed and socked connection is new
    if self.auth and sock:getreusedtimes() == 0 then -- SASL AUTH
        local ok, auth, err

        auth, err = auth_utils.new(self.auth)
        if not auth then
            return nil, err
        end

        ok, err = auth:authenticate(sock)
        if not ok then
            local msg = "failed to do " .. self.auth.mechanism .." auth with " ..
                        self.host .. ":" .. tostring(self.port) .. ": " .. err, true
            return nil, msg
        end
    end

    local data, err, f  = _sock_send_receive(sock, request)
    sock:setkeepalive(self.config.keepalive_timeout, self.config.keepalive_size)
    return data, err, f
end


function _sock_send_receive(sock, request)
    local bytes, err = sock:send(request:package())
    if not bytes then
        return nil, err, true
    end

    -- Reading a 4 byte `message_size`
    local len, err = sock:receive(4)

    if not len then
        if err == "timeout" then
            sock:close()
            return nil, err
        end
        return nil, err, true
    end

    local data, err = sock:receive(to_int32(len))
    if not data then
        if err == "timeout" then
            sock:close()
        return nil, err, true
        end
    end

    return response:new(data, request.api_version), nil, true
end

return _M
