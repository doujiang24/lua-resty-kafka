-- Copyright (C) Dejiang Zhu(doujiang24)


local bit = require "bit"


local setmetatable = setmetatable
local byte = string.byte
local sub = string.sub
local lshift = bit.lshift
local bor = bit.bor


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 9)
_M._VERSION = '0.01'


function _M.new(self, str)
    local resp = setmetatable({
        str = str,
        offset = 1,
        correlation_id = 0,
    }, { __index = _M })

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
    return bor(lshift(byte(str, offset), 24),
               lshift(byte(str, offset + 1), 16),
               lshift(byte(str, offset + 2), 8),
               byte(str, offset + 3))
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
    local str = self.str
    local offset = self.offset
    self.offset = offset + 4

    return 4294967296LL *
            bor(lshift(byte(str, offset), 56),
                lshift(byte(str, offset + 1), 48),
                lshift(byte(str, offset + 2), 40),
                lshift(byte(str, offset + 3), 32))
            + 16777216LL * byte(str, offset + 4)
            + bor(lshift(byte(str, offset + 5), 16),
                lshift(byte(str, offset + 6), 8),
                byte(str, offset + 7))
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
