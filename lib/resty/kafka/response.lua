-- Copyright (C) Dejiang Zhu(doujiang24)


local bit = require("bit")
local request = require("resty.kafka.request")


local setmetatable = setmetatable
local byte = string.byte
local sub = string.sub
local band = bit.band
local lshift = bit.lshift
local arshift = bit.arshift
local bor = bit.bor
local bxor = bit.bxor
local strbyte = string.byte
local floor = math.floor


local _M = {}
local mt = { __index = _M }


function _M.new(self, str, api_version)
    local resp = setmetatable({
        str = str,
        offset = 1,
        correlation_id = 0,
        api_version = api_version,
    }, mt)

    resp.correlation_id = resp:int32()

    return resp
end


local function _int8(str, offset)
    return byte(str, offset)
end


function _M.int8(self)
    local str = self.str
    local offset = self.offset
    self.offset = offset + 1
    return _int8(str, offset)
end


local function _int16(str, offset)
    local high = byte(str, offset)
    -- high padded
    return bor((high >= 128) and 0xffff0000 or 0,
            lshift(high, 8),
            byte(str, offset + 1))
end


function _M.int16(self)
    local str = self.str
    local offset = self.offset
    self.offset = offset + 2

    return _int16(str, offset)
end


local function _int32(str, offset)
    local offset = offset or 1
    local a, b, c, d = strbyte(str, offset, offset + 3)
    return bor(lshift(a, 24), lshift(b, 16), lshift(c, 8), d)
end
_M.to_int32 = _int32


function _M.int32(self)
    local str = self.str
    local offset = self.offset
    self.offset = offset + 4

    return _int32(str, offset)
end


local function _int64(str, offset)
    local a, b, c, d, e, f, g, h = strbyte(str, offset, offset + 7)

    --[[
    -- only 52 bit accuracy
    local hi = bor(lshift(a, 24), lshift(b, 16), lshift(c, 8), d)
    local lo = bor(lshift(f, 16), lshift(g, 8), h)
    return hi * 4294967296 + 16777216 * e + lo
    --]]

    return 4294967296LL * bor(lshift(a, 56), lshift(b, 48), lshift(c, 40), lshift(d, 32))
            + 16777216LL * e
            + bor(lshift(f, 16), lshift(g, 8), h)
end


-- XX return cdata: LL
function _M.int64(self)
    local str = self.str
    local offset = self.offset
    self.offset = offset + 8

    return _int64(str, offset)
end


-- Get a fixed-length integer from an offset position without
-- modifying the global offset of the response
-- The lengths of offset and length are in byte
function _M.peek_int(self, peek_offset, length)
    local str = self.str
    local offset = self.offset + peek_offset

    if length == 8 then
        return _int64(str, offset)
    elseif length == 4 then
        return _int32(str, offset)
    elseif length == 2 then
        return _int16(str. offset)
    else
        return _int8(str, offset)
    end
end


function _M.string(self)
    local len = self:int16()
    -- len = -1 means null
    if len < 0 then
        return nil
    end

    local offset = self.offset
    self.offset = offset + len

    return sub(self.str, offset, offset + len - 1)
end


function _M.bytes(self)
    local len = self:int32()
    if len < 0 then
        return nil
    end

    local offset = self.offset
    self.offset = offset + len

    return sub(self.str, offset, offset + len - 1)
end


function _M.correlation_id(self)
    return self.correlation_id
end


-- Take 1 byte (8 bit) from offset without modifying the offset
local function _byte(self, offset)
    offset = offset or self.offset
    return sub(self.str, offset, offset)
end


return _M
