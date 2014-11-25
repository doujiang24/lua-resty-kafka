-- Copyright (C) Dejiang Zhu(doujiang24)


local setmetatable = setmetatable


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local MAX_REUSE = 10000
local INIT_SIZE = 20000


local _M = new_tab(0, 15)
_M._VERSION = '0.01'


function _M.new(self, opts)
    local buffer = {
        data = new_tab(INIT_SIZE, 0),
        flush_size = opts.flush_size,
        max_size = opts.max_size,   -- should be less than socket.request.max.bytes
                                    -- config in the kafka server, default 100M
        index = 0,
        used = 0,
        size = 0,
    }
    return setmetatable(buffer, { __index = _M })
end


function _M.add(self, messages)
    local mlen = #messages
    local index = self.index

    local size = 0
    local data = self.data
    for i = 1, mlen do
        size = size + #messages[i]
        data[index + i] = messages[i]
    end

    if self.size + size > self.max_size then
        return nil, "buffer size overflow"
    end

    self.size = self.size + size
    self.index = index + mlen
    return true, size
end


function _M.need_flush(self)
    return self.size >= self.flush_size
end


function _M.flush(self)
    local data = self.data
    local index = self.index

    if index == 0 then
        return nil, 0
    end

    self.size = 0
    self.index = 0
    self.used = self.used + 1

    if self.used > MAX_REUSE then
        self.data = new_tab(INIT_SIZE, 0)
        self.used = 0
    end

    return data, index
end


return _M
