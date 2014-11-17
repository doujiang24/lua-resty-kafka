-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"


local to_int32 = response.to_int32
local setmetatable = setmetatable
local tcp = ngx.socket.tcp


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 6)
_M._VERSION = '0.01'


local mt = { __index = _M }


function _M.new(self, ...)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end

    local ok, err = sock:connect(...)
    if not ok then
        return nil, err
    end

    return setmetatable({ sock = sock }, mt)
end


function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function _M.set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function _M.close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


function _M.send_receive(self, request)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send(request:package())
    if not bytes then
        return nil, err
    end

    local data, err, partial = sock:receive(4)
    if not data then
        return nil, err
    end

    local len = to_int32(data)

    local data, err, partial = sock:receive(len)
    if not data then
        return nil, err
    end

    return response:new(data)
end


return _M
