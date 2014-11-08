-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local broker = require "resty.kafka.broker"


local setmetatable = setmetatable
local ngx_sleep = ngx.sleep
local ngx_log = ngx.log
local DEBUG = ngx.DEBUG
local ERR = ngx.ERR
local debug = ngx.config.debug


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local REQUIRED_ACKS = 1


local _M = new_tab(0, 3)
_M._VERSION = '0.01'


local mt = { __index = _M }


function _M.new(self, broker_list, opts)
    local opts = opts or {}

    return setmetatable({
        correlation_id = 1,
        broker_list = broker_list,
        topic_partitions = {},
        broker_nodes = {},
        client_id = opts.client_id or "client",
        request_timeout = opts.request_timeout or 2000,
        retry_interval = opts.retry_interval or 100,   -- ms
        max_retry = opts.max_retry or 3,
    }, mt)
end


local function correlation_id(self)
    local id = (self.correlation_id + 1) % 1073741824 -- 2^30
    self.correlation_id = id

    return id
end


local function produce_encode(self, topic, messages, index)
    local timeout = self.request_timeout
    local client_id = self.client_id
    local id = correlation_id(self)

    local req = request:new(request.ProduceRequest, id, client_id)

    --XX hard code for requiredAcks
    req:int16(REQUIRED_ACKS)
    req:int32(timeout)

    -- XX hard code for topic num
    req:int32(1)
    req:string(topic)

    -- XX hard code for partition, modified in func: send
    req:int32(1)
    req:int32(0)

    -- MessageSetSize and MessageSet
    req:message_set(messages, index)

    return req
end


local function produce_decode(resp)
    local ret = {}

    local topic_num = resp:int32()

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


local function metadata_encode(self, topic)
    local client_id = self.client_id
    local id = correlation_id(self)

    local req = request:new(request.MetadataRequest, id, client_id)

    -- topic num is 1
    req:int32(1)
    req:string(topic)

    return req
end


local function metadata_decode(resp)
    local bk_num = resp:int32()
    local brokers = new_tab(0, bk_num)

    for i = 1, bk_num do
        local nodeid = resp:int32();
        brokers[nodeid] = {
            host = resp:string(),
            port = resp:int32(),
        }
    end

    local topic_num = resp:int32()
    local topics = new_tab(0, topic_num)

    for i = 1, topic_num do
        local tp_errcode = resp:int16()
        local topic = resp:string()

        local partition_num = resp:int32()
        local topic_info = {
            partitions = new_tab(partition_num, 0),
            errcode = tp_errcode,
            num = partition_num,
        }

        for j = 1, partition_num do
            local partition_info = new_tab(0, 5)

            partition_info.errcode = resp:int16()
            partition_info.id = resp:int32()
            partition_info.leader = resp:int32()

            local repl_num = resp:int32()
            partition_info.replicas = new_tab(repl_num, 0)

            for m = 1, repl_num do
                partition_info.replicas[m] = resp:int32()
            end

            local isr_num = resp:int32()
            partition_info.isr = new_tab(isr_num, 0)

            for m = 1, isr_num do
                partition_info.isr[m] = resp:int32()
            end

            topic_info.partitions[j] = partition_info
        end
        topics[topic] = topic_info
    end

    return { brokers = brokers, topics = topics }
end


local function _fetch_metadata(self, topic)
    local broker_list = self.broker_list

    for i = 1, #broker_list do
        local host, port = broker_list[i].host, broker_list[i].port
        local bk, err = broker:new(host, port)

        if not bk then
            ngx_log(ERR, "broker connect failed, err:", err, host, port)
        else
            local req = metadata_encode(self, topic)

            local resp, err = bk:send_receive(req)
            bk:keepalive()

            if not resp then
                ngx_log(ERR, "broker metadata failed, err:", err, host, port)

            else
                local meta = metadata_decode(resp)
                local info = meta.topics[topic]

                self.topic_partitions[topic] = info
                self.broker_nodes = meta.brokers

                return info
            end
        end
    end

    ngx_log(ERR, "refresh metadata failed")
    return nil, "refresh metadata failed"
end


local function fetch_metadata(self, topic)
    local partitions = self.topic_partitions[topic]
    if partitions then
        return partitions
    end

    return _fetch_metadata(self, topic)
end


local function choose_partition(self, topic)
    local partitions, err = fetch_metadata(self, topic)
    if not partitions then
        return nil, err
    end

    local partition = partitions.partitions[self.correlation_id % partitions.num + 1]

    local id = partition.id
    local leader = partition.leader
    local config = self.broker_nodes[leader]

    local bk, err = broker:new(config.host, config.port)
    if not bk then
        return nil, err
    end

    return bk, id
end


function _M.send(self, topic, messages, index)
    local req = produce_encode(self, topic, messages, index)

    local retry, resp, err = 0

    while retry <= self.max_retry do
        local broker, partition = choose_partition(self, topic)
        if broker then
            req:partition(partition)
            resp, err = broker:send_receive(req)
            broker:keepalive()
            if resp then
                return produce_decode(resp)
            end
        end

        if debug then
            ngx_log(DEBUG, "retry to send messages to kafka server: ", err)
        end

        ngx_sleep(self.retry_interval / 1000)
        fetch_metadata(self, topic, true)

        retry = retry + 1
    end

    return nil, err
end


return _M
