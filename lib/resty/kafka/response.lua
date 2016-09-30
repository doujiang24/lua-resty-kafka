-- Copyright (C) Dejiang Zhu(doujiang24)


local bit = require "bit"


local setmetatable = setmetatable
local byte = string.byte
local sub = string.sub
local lshift = bit.lshift
local bor = bit.bor
local strbyte = string.byte


local _M = {}
local mt = { __index = _M }


function _M.new(self, str)
    local resp = setmetatable({
        str = str,
        offset = 1,
        correlation_id = 0,
    }, mt)

    resp.correlation_id = resp:int32()

    return resp
end


function _M.int16(self)
    local str = self.str
    local offset = self.offset
    self.offset = offset + 2

    local high = byte(str, offset)
    -- high padded
    return bor((high >= 128) and 0xffff0000 or 0,
            lshift(high, 8),
            byte(str, offset + 1))
end


local function to_int32(str, offset)
    local offset = offset or 1
    local a, b, c, d = strbyte(str, offset, offset + 3)
    return bor(lshift(a, 24), lshift(b, 16), lshift(c, 8), d)
end
_M.to_int32 = to_int32


function _M.int32(self)
    local str = self.str
    local offset = self.offset
    self.offset = offset + 4

    return to_int32(str, offset)
end


-- XX return cdata: LL
function _M.int64(self)
    local offset = self.offset
    self.offset = offset + 8

    local a, b, c, d, e, f, g, h = strbyte(self.str, offset, offset + 7)

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


function _M.string(self)
    local len = self:int16()

    local offset = self.offset
    self.offset = offset + len

    return sub(self.str, offset, offset + len - 1)
end


function _M.bytes(self)
    local len = self:int32()

    local offset = self.offset
    self.offset = offset + len

    return sub(self.str, offset, offset + len - 1)
end


function _M.correlation_id(self)
    return self.correlation_id
end


return _M
