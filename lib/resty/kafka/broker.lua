-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"


local to_int32 = response.to_int32
local setmetatable = setmetatable
local tcp = ngx.socket.tcp


local _M = {}
local mt = { __index = _M }


function _M.new(self, host, port, socket_config)
    return setmetatable({
        host = host,
        port = port,
        config = socket_config,
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
        local _, err = sock:sslhandshake(false, self.host, self.config.ssl_verify)
        if err then
            return nil, "failed to do SSL handshake with " ..
                        self.host .. ":" .. tostring(self.port) .. ": " .. err, true
        end
    end

    local bytes, err = sock:send(request:package())
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

    sock:setkeepalive(self.config.keepalive_timeout, self.config.keepalive_size)

    return response:new(data, request.api_version), nil, true
end


return _M
