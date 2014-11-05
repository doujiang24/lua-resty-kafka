-- Copyright (C) Dejiang Zhu(doujiang24)


local setmetatable = setmetatable


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 15)
_M._VERSION = '0.01'


function _M.new(self, opts)
    local opts = opts or {}
    local max_length = opts.max_length or 10000
    local flush_size = opts.flush_size or 10240 -- 10KB
    local flush_length = opts.flush_length or 100
    local max_size = opts.max_size or  10485760 -- 10MB
    local max_reuse = opts.max_reuse or 10000

    local buffer = {
        data = new_tab(max_length, 0),
        flush_size = flush_size,   -- 10KB
        flush_length = flush_length,
        max_size = max_size,   -- 10MB
        max_length = max_length,
        max_reuse = max_reuse,
        index = 0,
        used = 0,
        size = 0,
    }
    return setmetatable(buffer, { __index = _M })
end


function _M.add(self, messages)
    local mlen = #messages
    local index = self.index
    local new_index = index + mlen

    if new_index > self.max_length then
        return nil, "buffer length overflow"
    end

    local size = self.size
    local data = self.data
    for i = 1, mlen do
        size = size + #messages[i]
        data[index + i] = messages[i]
    end

    if size > self.max_size then
        return nil, "buffer size overflow"
    end

    self.size = size
    self.index = new_index
    return new_index
end


function _M.need_flush(self)
    return (self.index >= self.flush_size) or (self.size >= self.flush_size)
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

    if self.used > self.max_reuse then
        self.data = new_tab(self.max_length, 0)
    end

    return data, index
end


return _M
