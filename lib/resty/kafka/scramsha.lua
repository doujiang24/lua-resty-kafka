local request = require("resty.kafka.request")
local response = require("resty.kafka.response")
local ngx_re = require("ngx.re")
local ngx = ngx
local pid = ngx.worker.pid
local str_gsub = ngx.re.gsub
local to_int32 = response.to_int32
local string = string
local table = table
local ipairs = ipairs
local tonumber = tonumber
local tostring = tostring
local setmetatable = setmetatable

local scram_sha_type = {
    ["SCRAM-SHA-256"]={
        ["name"]="sha256",
        ["out_len"]=32
    },
    ["SCRAM-SHA-512"]={
        ["name"]="sha512",
        ["out_len"]=64
    }
}

local _M = {}

local mt = { __index = _M }

function _M.new(sock, user,password)
    local self = {
        user = user,
        password = password,
        sock = sock
    }
    return setmetatable(self, mt)
end

local rshift, lshift, band, bxor
do
    local _obj_0 = require("bit")
    rshift, lshift, band = _obj_0.rshift, _obj_0.lshift, _obj_0.band
    bxor = _obj_0.bxor
end

local function pbkdf2_hmac(scram_sha_conf,str, salt, i)
    local openssl_kdf = require("resty.openssl.kdf")

    salt = ngx.decode_base64(salt)

    local key, err = openssl_kdf.derive({
        type = openssl_kdf.PBKDF2,
        md = scram_sha_conf.name,
        salt = salt,
        pbkdf2_iter = i,
        pass = str,
        outlen = scram_sha_conf.out_len -- our H() produces a 64 byte hash value (SHA-512)  SHA-256=32
    })

    if not (key) then
        return nil, "failed to derive pbkdf2 key: " .. tostring(err)
    end
    return key
end

local function hmac(scram_sha_type,key, str)
    local openssl_hmac = require("resty.openssl.hmac")
    local hmac, err = openssl_hmac.new(key, scram_sha_type)

    if not (hmac) then
        return nil, tostring(err)
    end

    hmac:update(str)

    local final_hmac, err = hmac:final()

    if not (final_hmac) then
        return nil, tostring(err)
    end

    return final_hmac
end

local function hash_func(scram_sha_type,str)
    local openssl_digest, err = require("resty.openssl.digest").new(scram_sha_type)

    if not (openssl_digest) then
        return nil, tostring(err)
    end

    openssl_digest:update(str)

    local digest, err = openssl_digest:final()

    if not (digest) then
        return nil, tostring(err)
    end

    return digest
end

local function xor(a, b)
    local result = {}
    for i = 1, #a do
        local x = a:byte(i)
        local y = b:byte(i)

        if not (x) or not (y) then
            return
        end

        result[i] = string.char(bxor(x, y))
    end

    return table.concat(result)
end

function _M.scram_sha_auth(self, msg)
    local uuid = require("resty.jit-uuid")
    local c_nonce = str_gsub(uuid(),"-","")
    local nonce = "r=" .. c_nonce
    local sasl_name = self.user
    local username = "n=" .. sasl_name
    local auth_message = username .. "," .. nonce

    local scram_sha_conf = scram_sha_type[msg]
    if not scram_sha_conf then
        return nil, "unsupported SCRAM mechanism name: " .. tostring(msg)
    end

    local gs2_header = "n,,"
    local client_first_message = gs2_header .. auth_message

    local t, server_first_message = self:send_first_message(client_first_message)
    if not (t) then
        return nil, server_first_message
    end

    auth_message = auth_message .. ',' .. server_first_message
    local pairs = ngx_re.split(server_first_message,",")
    if not pairs or #pairs == 0 then
        return nil, "server_first_message error,message:" .. server_first_message
    end

    local params = {}
    for _,v in ipairs(pairs) do
        local dict = ngx_re.split(v,"=")
        local key = dict[1]
        local value = dict[2]
        params[key] = value
    end

    local server_nonce = params["r"]
    local from, _, _ = ngx.re.find(server_nonce, c_nonce, "jo")
    if not from then
        return nil, "Server nonce, did not start with client nonce!"
    end

    auth_message = auth_message .. ',c=biws,r=' .. server_nonce
    local salt = params['s']
    local length = tonumber(params['i'])
    if length < 4096 then
        return nil, "the iteration-count sent by the server is less than 4096"
    end

    local salted_password, err = pbkdf2_hmac(scram_sha_conf,self.password, salt, length)
    if not (salted_password) then
        return nil, tostring(err)
    end
    local client_key, err = hmac(scram_sha_conf.name,salted_password, "Client Key")
    if not (client_key) then
        return nil, tostring(err)
    end
    local stored_key, err = hash_func(scram_sha_conf.name,client_key)
    if not (stored_key) then
        return nil, tostring(err)
    end
    local client_signature, err = hmac(scram_sha_conf.name,stored_key, auth_message)
    if not (client_signature) then
        return nil, tostring(err)
    end
    local proof = xor(client_key, client_signature)
    if not (proof) then
        return nil, "failed to generate the client proof"
    end
    local client_final_message = 'c=biws,r=' .. server_nonce .. ",p=" .. ngx.encode_base64(proof)
    return true, client_final_message
end

function _M.sock_send_receive(self, request)
    local sock = self.sock
    local req = request:package()
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err, true
    end
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
            return nil, err
        end
        return nil, err, true
    end
    return response:new(data, request.api_version), nil, true
end

function _M.send_first_message(self,msg)
    local cli_id = "worker" .. pid()
    local req = request:new(request.SaslAuthenticateRequest, 0, cli_id,
            request.API_VERSION_V1)
    req:bytes(msg)

    local resp, err = self:sock_send_receive(req)
    if not resp  then
        return nil, err
    end
    local err_code = resp:int16()
    local error_msg = resp:string()
    local auth_bytes = resp:bytes()
    if err_code ~= 0 then
        return nil, error_msg
    end
    return true, auth_bytes
end

return _M