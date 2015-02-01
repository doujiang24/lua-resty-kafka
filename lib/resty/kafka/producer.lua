-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local broker = require "resty.kafka.broker"
local buffer = require "resty.kafka.buffer"
local client = require "resty.kafka.client"
local Errors = require "resty.kafka.errors"


local setmetatable = setmetatable
local timer_at = ngx.timer.at
local is_exiting = ngx.worker.exiting
local ngx_sleep = ngx.sleep
local ngx_log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local DEBUG = ngx.DEBUG
local debug = ngx.config.debug
local crc32 = ngx.crc32_short


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 4)
_M._VERSION = '0.01'


local mt = { __index = _M }


local cluster_inited


local function default_partitioner(key, num, correlation_id)
    local id = key and crc32(key) or correlation_id

    -- partition_id is continuous and start from 0
    return id % num
end


local function correlation_id(self)
    local id = (self.correlation_id + 1) % 1073741824 -- 2^30
    self.correlation_id = id

    return id
end


local function produce_encode(self, topic, partition_id, queue, index)
    local req = request:new(request.ProduceRequest,
                            correlation_id(self), self.client.client_id)

    req:int16(self.required_acks)
    req:int32(self.request_timeout)

    -- XX hard code for topic num: one topic one send
    req:int32(1)
    req:string(topic)

    -- XX hard code for partition num, one partition on send
    req:int32(1)
    req:int32(partition_id)

    -- MessageSetSize and MessageSet
    req:message_set(queue, index)

    return req
end


local function produce_decode(resp)
    local topic_num = resp:int32()
    local ret = new_tab(0, topic_num)

    for i = 1, topic_num do
        local topic = resp:string()
        local partition_num = resp:int32()

        ret[topic] = {}

        for j = 1, partition_num do
            local partition = resp:int32()

            ret[topic][partition] = {
                errcode = resp:int16(),
                offset = resp:int64(),
            }
        end
    end

    return ret
end


local function choose_partition(self, topic, key)
    local brokers, partitions = self.client:fetch_metadata(topic)
    if not brokers then
        return nil, partitions
    end

    return self.partitioner(key, partitions.num, self.correlation_id)
end


local function choose_broker(self, topic, partition_id)
    local brokers, partitions = self.client:fetch_metadata(topic)
    if not brokers then
        return nil, partitions
    end

    local partition = partitions[partition_id]
    if not partition then
        return nil, "not found partition"
    end

    local config = brokers[partition.leader]
    if not config then
        return nil, "not found broker"
    end

    return broker:new(config.host, config.port, self.socket_config)
end


-- queue is array table {key, msg, key, msg ...}
-- key can not be nil, can be ""
local function _send(self, topic, partition_id, queue, index)
    local req = produce_encode(self, topic, partition_id, queue, index)

    local retry, retryable, resp, bk, err = 1, true

    while retryable and retry <= self.max_retry do
        bk, err = choose_broker(self, topic, partition_id)
        if bk then
            resp, err, retryable = bk:send_receive(req)
            if resp then
                local r = produce_decode(resp)[topic][partition_id]
                local errcode = r.errcode
                if errcode == 0 then
                    return r.offset
                else
                    err = Errors[errcode]

                    -- XX: only 3, 5, 6 can retry
                    if errcode ~= 3 and errcode ~= 5 and errcode ~= 6 then
                        retryable = nil
                    end
                end
            end
        end

        ngx_log(INFO, "retry to send messages to kafka err: ", err, ", retryable: ", retryable,
                ", topic: ", topic, ", partition_id: ", partition_id, ", length: ", index / 2)

        ngx_sleep(self.retry_interval / 1000)   -- ms to s
        self.client:refresh()

        retry = retry + 1
    end

    return nil, err, retryable
end


local function _flush_lock(self)
    if not self.flushing then
        if debug then
            ngx_log(DEBUG, "flush lock accquired")
        end
        self.flushing = true
        return true
    end
    return false
end


local function _flush_unlock(self)
    if debug then
        ngx_log(DEBUG, "flush lock released")
    end
    self.flushing = false
end


local function _flush(premature, self, force)
    if not _flush_lock(self) then
        if debug then
            ngx_log(DEBUG, "previous flush not finished")
        end

        if not force then
            return true
        end

        repeat
            if debug then
                ngx_log(DEBUG, "last flush required lock")
            end
            ngx_sleep(0.1)
        until _flush_lock(self)
    end

    local send_num = 0
    for topic, buffers in pairs(self.buffers) do
        for partition_id, bf in pairs(buffers) do
            if force or bf:need_flush() then
                -- get queue
                local queue, index = bf:flush()

                -- batch send queue
                if index > 0 then
                    local offset, err, retryable = _send(self, topic, partition_id, queue, index)
                    if offset then
                        send_num = send_num + (index / 2)
                    else
                        if self.error_handle then
                            self.error_handle(topic, partition_id, queue, index, err, retryable)
                        else
                            ngx_log(ERR, "buffered messages send to kafka err: ", err,
                                        ", retryable: ", retryable, ", topic: ", topic,
                                        ", partition_id: ", partition_id, ", length: ", index / 2)
                        end
                    end
                end
            end
        end
    end

    _flush_unlock(self)
    return send_num
end


local function _flush_buffer(self, force)
    local ok, err = timer_at(0, _flush, self, force)
    if not ok then
        ngx_log(ERR, "failed to create timer at _flush_buffer, err: ", err)
    end
end


local _timer_flush
_timer_flush = function (premature, self, time)
    _flush(nil, self, true)

    if is_exiting() then
        return _flush(nil, self, true)
    end

    local ok, err = timer_at(time, _timer_flush, self, time)
    if not ok then
        ngx_log(ERR, "failed to create timer at _timer_flush, err: ", err)
    end
end


local function _buffer_add(self, topic, partition_id, key, message)
    local buffers = self.buffers
    if not buffers[topic] then
        buffers[topic] = {}
    end
    if not buffers[topic][partition_id] then
        buffers[topic][partition_id] = buffer:new(self.buffer_opts)
    end

    local bf = buffers[topic][partition_id]
    local size, err = bf:add(key, message)
    if not size then
        return nil, err
    end

    local force = is_exiting()
    if force or bf:need_flush() then
        _flush_buffer(self, force)
    end

    return size
end


function _M.new(self, broker_list, producer_config)
    local opts = producer_config or {}
    local async = opts.producer_type == "async"
    if async and cluster_inited then
        return cluster_inited
    end

    local buffer_opts = {
        flush_time = opts.flush_time or 1000,   -- 1s
        flush_size = opts.flush_size or 1024,  -- 1KB
        max_size = opts.max_size or 1048576,    -- 1MB
                -- should less than (MaxRequestSize - 10KiB)
                -- config in the kafka server, default 100M
    }

    local cli = client:new(broker_list, producer_config)
    local p = setmetatable({
        client = cli,
        correlation_id = 1,
        request_timeout = opts.request_timeout or 2000,
        retry_interval = opts.retry_interval or 100,   -- ms
        max_retry = opts.max_retry or 3,
        required_acks = opts.required_acks or 1,
        partitioner = opts.partitioner or default_partitioner,
        error_handle = opts.error_handle,
        buffers = {},
        async = async,
        -- buffer config
        buffer_opts = buffer_opts,
        -- socket config
        socket_config = cli.socket_config,
    }, mt)

    if async then
        cluster_inited = p
        _timer_flush(nil, p, buffer_opts.flush_time / 1000)
    end
    return p
end


function _M.send(self, topic, key, message)
    local partition_id, err = choose_partition(self, topic, key)
    if not partition_id then
        return nil, err
    end

    if self.async then
        return _buffer_add(self, topic, partition_id, key, message)
    end

    local queue = { key or "", message }
    return _send(self, topic, partition_id, queue, 2)
end


function _M.flush(self)
    return _flush(nil, self, true)
end


return _M
