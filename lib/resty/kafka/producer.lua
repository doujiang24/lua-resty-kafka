-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local client = require "resty.kafka.client"
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


local _M = new_tab(0, 3)
_M._VERSION = '0.01'


local mt = { __index = _M }


function _M.new(self, broker_list, opts)
    local opts = opts or {}
    local request_timeout = opts.request_timeout or 2000
    local socket_config = {
        socket_timeout = opts.socket_timeout or (request_timeout + 1000),
        keepalive_timeout = opts.keepalive_timeout or 600 * 1000, -- 10 min
        keepalive_size = opts.keepalive_size or 2,
    }

    local cli = client:new(broker_list, opts.metadata_refresh_interval, socket_config)

    return setmetatable({
        client = cli,
        correlation_id = 1,
        request_timeout = request_timeout,
        retry_interval = opts.retry_interval or 100,   -- ms
        max_retry = opts.max_retry or 3,
        required_acks = opts.required_acks or 1,
        -- socket config
        socket_config = socket_config,
    }, mt)
end


local function correlation_id(self)
    local id = (self.correlation_id + 1) % 1073741824 -- 2^30
    self.correlation_id = id

    return id
end


local function produce_encode(self, topic, messages, index)
    local timeout = self.request_timeout
    local id = correlation_id(self)

    local req = request:new(request.ProduceRequest, id, self.client.client_id)

    req:int16(self.required_acks)
    req:int32(timeout)

    -- XX hard code for topic num: one topic one send
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


local function choose_partition(self, topic)
    local brokers, partitions = self.client:fetch_metadata(topic)
    if not brokers then
        return nil, partitions
    end

    local partition = partitions.partitions[self.correlation_id % partitions.num + 1]

    local id = partition.id
    local config = brokers[partition.leader]

    local sc = self.socket_config
    local bk, err = broker:new(config.host, config.port, sc.socket_timeout,
                                sc.keepalive_timeout, sc.keepalive_size)
    if not bk then
        return nil, err
    end

    return bk, id
end


function _M.send(self, topic, messages, index)
    local req = produce_encode(self, topic, messages, index)

    local retry, resp, err = 0

    while retry <= self.max_retry do
        local bk, partition = choose_partition(self, topic)
        if bk then
            req:partition(partition)
            resp, err = bk:send_receive(req)
            bk:set_keepalive()
            if resp then
                return produce_decode(resp)
            end
        else
            err = partition
        end

        if debug then
            ngx_log(DEBUG, "retry to send messages to kafka server: ", err)
        end

        ngx_sleep(self.retry_interval / 1000)
        self.client:refresh()

        retry = retry + 1
    end

    return nil, err
end


return _M
