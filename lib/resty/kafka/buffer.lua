-- Copyright (C) Dejiang Zhu(doujiang24)


local setmetatable = setmetatable


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 15)
_M._VERSION = '0.01'


function _M.new(self, opts)
    local buffer = {
        data = new_tab(opts.max_length, 0),
        flush_size = opts.flush_size,
        flush_length = opts.flush_length,
        max_size = opts.max_size,
        max_length = opts.max_length,
        max_reuse = opts.max_reuse,
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
    return true
end


function _M.need_flush(self)
    return (self.index >= self.flush_length) or (self.size >= self.flush_size)
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
        self.used = 0
    end

    return data, index
end


return _M
