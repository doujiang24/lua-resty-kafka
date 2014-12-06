-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local broker = require "resty.kafka.broker"
local client = require "resty.kafka.client"
local Errors = require "resty.kafka.errors"


local setmetatable = setmetatable
local ngx_sleep = ngx.sleep
local ngx_log = ngx.log
local DEBUG = ngx.DEBUG
local debug = ngx.config.debug
local crc32 = ngx.crc32_short


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 5)
_M._VERSION = '0.01'


local mt = { __index = _M }


local function default_partitioner(key, num, correlation_id)
    local id = key and crc32(key) or correlation_id

    -- partition_id is continuous and start from 0
    return id % num
end


function _M.new(self, broker_list, client_config, producer_config)
    local opts = producer_config or {}
    local cli = client:new(broker_list, client_config)

    return setmetatable({
        client = cli,
        correlation_id = 1,
        request_timeout = opts.request_timeout or 2000,
        retry_interval = opts.retry_interval or 100,   -- ms
        max_retry = opts.max_retry or 3,
        required_acks = opts.required_acks or 1,
        partitioner = opts.partitioner or default_partitioner,
        -- socket config
        socket_config = cli.socket_config,
        -- producer broker instance
        producer_brokers = {},
    }, mt)
end


local function correlation_id(self)
    local id = (self.correlation_id + 1) % 1073741824 -- 2^30
    self.correlation_id = id

    return id
end


local function produce_encode(self, topic, partition_id, messages, index)
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
    req:message_set(messages, index)

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
_M.choose_partition = choose_partition


local function choose_broker(self, topic, partition_id)
    local brokers, partitions = self.client:fetch_metadata(topic)
    if not brokers then
        return nil, partitions
    end

    -- lua array table start from 1
    local partition = partitions[partition_id + 1]
    if not partition then
        return nil, "not found partition"
    end

    local leader = partition.leader
    local bk = self.producer_brokers[leader]
    if bk then
        return bk
    end

    local config = brokers[leader]
    local bk = broker:new(config.host, config.port, self.socket_config)
    self.producer_brokers[leader] = bk

    return bk
end


-- messages is array table {key, msg, key, msg ...}
-- key can not be nil, can be ""
local function batch_send(self, topic, partition_id, messages, index)
    local req = produce_encode(self, topic, partition_id,
                                messages, index or #messages)

    local retry, resp, bk, err = 1

    while retry <= self.max_retry do
        bk, err = choose_broker(self, topic, partition_id)
        if bk then
            resp, err = bk:send_receive(req)
            if resp then
                local r = produce_decode(resp)[topic][partition_id]
                if r.errcode == 0 then
                    return r.offset
                else
                    err = Errors[r.errcode]
                end
            end
        end

        if debug then
            ngx_log(DEBUG, "retry to send messages to kafka err: ", err,
                    ", topic: ", topic, ", partition_id: ", partition_id)
        end

        ngx_sleep(self.retry_interval / 1000)
        self.client:refresh()

        retry = retry + 1
    end

    return nil, err
end
_M.batch_send = batch_send


function _M.send(self, topic, key, message)
    local partition_id, err = choose_partition(self, topic, key)
    if not partition_id then
        return nil, err
    end

    local messages = { key or "", message }
    return batch_send(self, topic, partition_id, messages)
end


return _M
