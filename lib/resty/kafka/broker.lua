-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local sasl = require "resty.kafka.sasl"
local pid = ngx.worker.pid

local to_int32 = response.to_int32
local setmetatable = setmetatable
local tcp = ngx.socket.tcp


local _M = {}
local mt = { __index = _M }


function _M.new(self, host, port, socket_config, sasl_config)
    return setmetatable({
        host = host,
        port = port,
        config = socket_config,
        auth = sasl_config or nil,
    }, mt)
end

local function sasl_auth(sock, broker)
    local ok, err = _sasl_handshake(sock, broker)
    if not ok then
        if err then
            ngx.say("sasl handshake failed -> See list of supported Mechanisms: " .. err)
            return nil, err
        end
        return nil, "Unkown Error"
    end

    local ok, err = _sasl_auth(sock, broker)
    if not ok then
        return nil, err
    end
    return 0, nil
end

function _M.send_receive(self, request)
    local sock, err = tcp()
    if not sock then
        return nil, err, true
    end
    -- TODO: figure out how this works
    -- local times, err = sock:getreusedtimes()

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

    -- authenticate only if it is a new socket connection
    if self.auth and sock:getreusedtimes() == 0 then -- SASL AUTH
        local ok, err = sasl_auth(sock, self)
        if not ok then
            local msg = "failed to do " .. self.auth.mechanism .." auth with " ..
                        self.host .. ":" .. tostring(self.port) .. ": " .. err, true
            ngx.say("hello -> " .. msg)
            return nil, msg
        end
        ngx.say("Authentication successful")
    end

    local data, err, f  = _sock_send_recieve(sock, request)
    sock:setkeepalive(self.config.keepalive_timeout, self.config.keepalive_size)
    return data, err, f
end


function _sock_send_recieve(sock, request)
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

    ngx.say("Reading back response with api_version -> " .. request.api_version)
    return response:new(data, request.api_version), nil, true

end


function _sasl_handshake_decode(resp)
    -- TODO: contains mechanisms supported by the local server
    -- read this like I did with the supported api versions thing
    local err_code =  resp:int16()
    local mechanisms =  resp:string()
    ngx.say("Decoding sasl handshake response -> ")
    ngx.say("err_code -> " .. err_code)
    ngx.say("mechanisms -> " .. mechanisms)
    if err_code ~= 0 then
        return err_code, mechanisms
    end
    return 0, nil
end


function _sasl_auth_decode(resp)
    local err_code = resp:int16()
    local error_msg  = resp:nullable_string()
    ngx.say("sasl auth decode, error_code " .. err_code)
    ngx.say("sasl auth decode, error_message " .. error_msg)
    local auth_bytes  = resp:bytes()
    ngx.say("sasl auth decode, auth_bytes " .. auth_bytes)
    if err_code ~= 0 then
        return nil, error_msg
    end
    return 0, nil
end


function _sasl_auth(sock, brk)
    local cli_id = "worker" .. pid()
    local req = request:new(request.SaslAuthenticateRequest, 0, cli_id, request.API_VERSION_V1)
    local mechanism = brk.auth.mechanism
    ngx.say("Authenticating with mechanism -> " .. mechanism)
    local user = brk.auth.user
    ngx.say("Authenticating with user -> " .. user)
    local password = brk.auth.password
    ngx.say("Authenticating with pwd-> " .. password)
    local msg = sasl.encode(mechanism, user, password)
    req:bytes(msg)
    local resp, err = _sock_send_recieve(sock, req)
    if not resp  then
        ngx.say("Authentication failed with " .. err)
        return nil, err
    end
    ngx.say("Authentication succeeded, decoding sasl_auth response")
    local rc, err = _sasl_auth_decode(resp)
    if not rc then
        if err then
            return nil, err
        end
        return nil, "Unkown Error during _sasl_auth"
    end
    return rc, err
end


function _sasl_handshake(sock, brk)
    local cli_id = "worker" .. pid()
    local api_version = request.API_VERSION_V1

    local req = request:new(request.SaslHandshakeRequest, 0, cli_id, api_version)
    local mechanism = brk.auth.mechanism
    req:string(mechanism)
    ngx.say("Requesting handshake for mechanism -> " .. mechanism)
    local resp, err = _sock_send_recieve(sock, req)
    if not resp  then
        ngx.say("No response from sock_send_receive -> " .. err)
        return nil, err
    end
    local rc, mechanism = _sasl_handshake_decode(resp)
    -- the presence of mechanisms indicate that the mechanism used isn't enabled on the Kafka server.
    if mechanism then
        return nil, mechanism
    end
    return rc, nil
end


return _M
