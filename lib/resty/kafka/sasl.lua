local _M = {}

local scramsha = require "resty.kafka.scramsha"
local MECHANISM_PLAINTEXT = "PLAIN"
local MECHANISM_SCRAMSHA256 = "SCRAM-SHA-256" -- to do
local MECHANISM_SCRAMSHA512 = "SCRAM-SHA-512"
local SEP = string.char(0)


local function _encode_plaintext(authz_id, user, pwd)
    local msg = ""
    if authz_id then
        msg = msg ..authz_id
    end

    return (authz_id or "") .. SEP .. user .. SEP .. pwd
end


_M.encode = function(mechanism, authz_id, user, pwd,sock)
    if mechanism == MECHANISM_PLAINTEXT then
        return true, _encode_plaintext(authz_id, user, pwd)
    end
    if mechanism == MECHANISM_SCRAMSHA512 or mechanism == MECHANISM_SCRAMSHA256 then
        local scramsha_new = scramsha.new(sock,user,pwd)
        local ok, client_msg = scramsha_new:scram_sha_auth(mechanism)
        return ok, client_msg
    end
    return true, ""
end


return _M
