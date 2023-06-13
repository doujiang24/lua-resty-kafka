-- Copyright (C) Dejiang Zhu(doujiang24)

local semaphore = require "ngx.semaphore"

local setmetatable = setmetatable
local ngx_null = ngx.null

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = {}
local mt = { __index = _M }

function _M.new(self, batch_num, max_buffering, wait_on_buffer_full, wait_buffer_timeout)
    local sendbuffer = {
        queue = new_tab(max_buffering * 3, 0),
        batch_num = batch_num,
        size = max_buffering * 3,
        start = 1,
        num = 0,
        wait_on_buffer_full = wait_on_buffer_full,
        wait_buffer_timeout = wait_buffer_timeout,
    }

    if wait_on_buffer_full then
        sendbuffer.sema = semaphore.new()
    end

    return setmetatable(sendbuffer, mt)
end


function _M.add(self, topic, key, message)
    local num = self.num
    local size = self.size

    if num >= size then
        if not self.wait_on_buffer_full then
            return nil, "buffer overflow"
        end

        local timeout = self.wait_timeout
        local ok, err = self.sema:wait(timeout)
        if not ok then
            return nil, "buffer overflow " .. err
        end
        num = self.num
        size = self.size
    end

    local index = (self.start + num) % size
    local queue = self.queue

    queue[index] = topic
    queue[index + 1] = key
    queue[index + 2] = message

    self.num = num + 3

    return true
end


function _M.release_buffer_wait(self, num)
    if not self.wait_on_buffer_full then
        return
    end

    if self.sema:count() < 0 then
        self.sema:post(num)
    end
end


function _M.pop(self)
    local num = self.num
    if num <= 0 then
        return nil, "empty buffer"
    end

    self.num = num - 3

    local start = self.start
    local queue = self.queue

    self.start = (start + 3) % self.size

    local key, topic, message = queue[start], queue[start + 1], queue[start + 2]

    queue[start], queue[start + 1], queue[start + 2] = ngx_null, ngx_null, ngx_null

    -- It is enough to release a waiter as only one message pops up
    self:release_buffer_wait(1)

    return key, topic, message
end


function _M.left_num(self)
    return self.num / 3
end


function _M.need_send(self)
    return self.num / 3 >= self.batch_num
end


return _M
