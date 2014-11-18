-- Copyright (C) Dejiang Zhu(doujiang24)


local bit = require "bit"


local setmetatable = setmetatable
local concat = table.concat
local rshift = bit.rshift
local band = bit.band
local char = string.char
local crc32 = ngx.crc32_long


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 16)
_M._VERSION = '0.01'


local API_VERSION = 0

_M.ProduceRequest = 0
_M.FetchRequest = 1
_M.OffsetRequest = 2
_M.MetadataRequest = 3
_M.OffsetCommitRequest = 8
_M.OffsetFetchRequest = 9
_M.ConsumerMetadataRequest = 10


local function str_int8(int)
    return char(band(int, 0xff))
end


local function str_int16(int)
    return char(band(rshift(int, 8), 0xff),
                band(int, 0xff))
end


local function str_int32(int)
    -- ngx.say(debug.traceback())
    return char(band(rshift(int, 24), 0xff),
                band(rshift(int, 16), 0xff),
                band(rshift(int, 8), 0xff),
                band(int, 0xff))
end


local function str_int64(int)
    return char(band(rshift(int, 56), 0xff),
                band(rshift(int, 48), 0xff),
                band(rshift(int, 40), 0xff),
                band(rshift(int, 32), 0xff),
                band(rshift(int, 24), 0xff),
                band(rshift(int, 16), 0xff),
                band(rshift(int, 8), 0xff),
                band(int, 0xff))
end


function _M.new(self, apikey, correlation_id, client_id)
    local c_len = #client_id

    local req = {
        0,   -- request size: int32
        str_int16(apikey),
        str_int16(API_VERSION),
        str_int32(correlation_id),
        str_int16(c_len),
        client_id,
    }
    return setmetatable({
        _req = req,
        offset = 7,
        len = c_len + 10,
    }, { __index = _M })
end


function _M.int16(self, int)
    local req = self._req
    local offset = self.offset

    req[offset] = str_int16(int)

    self.offset = offset + 1
    self.len = self.len + 2
end


function _M.int32(self, int)
    local req = self._req
    local offset = self.offset

    req[offset] = str_int32(int)

    self.offset = offset + 1
    self.len = self.len + 4
end


function _M.string(self, str)
    local req = self._req
    local offset = self.offset
    local str_len = #str

    req[offset] = str_int16(str_len)
    req[offset + 1] = str

    self.offset = offset + 2
    self.len = self.len + 2 + str_len
end


function _M.bytes(self, str)
    local req = self._req
    local offset = self.offset
    local str_len = #str

    req[offset] = str_int32(str_len)
    req[offset + 1] = str

    self.offset = offset + 2
    self.len = self.len + 4 + str_len
end


local function message_package(msg)
    local len = #msg

    local req = {
        -- MagicByte
        str_int8(0),
        -- XX hard code no Compression
        str_int8(0),
        -- XX hard code key is null
        str_int32(-1),
        str_int32(len),
        msg,
    }

    local str = concat(req)
    return crc32(str), str, len + 14
end


function _M.partition(self, int)
    local req = self._req

    req[13] = str_int32(int)
end


function _M.message_set(self, messages, index)
    local req = self._req
    local off = self.offset + 1
    local msg_set_size = 0
    local index = index or #messages

    for i = 1, index do
        local crc32, str, msg_len = message_package(messages[i])

        req[off] = str_int64(0) -- offset
        req[off + 1] = str_int32(msg_len) -- include the crc32 length

        req[off + 2] = str_int32(crc32)
        req[off + 3] = str

        off = off + 4
        msg_set_size = msg_set_size + msg_len + 12
    end

    req[self.offset] = str_int32(msg_set_size) -- MessageSetSize

    req.offset = off
    self.len = self.len + 4 + msg_set_size
end


function _M.package(self)
    local req = self._req
    req[1] = str_int32(self.len)

    return concat(req)
end


return _M
