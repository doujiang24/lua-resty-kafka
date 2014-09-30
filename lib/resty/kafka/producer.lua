-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local broker = require "resty.kafka.broker"


local setmetatable = setmetatable
local ipairs = ipairs
local ngx_log = ngx.log
local ngx_info = ngx.INFO


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local REQUIRED_ACKS = 1


local _M = new_tab(0, 4)
_M._VERSION = '0.01'


local mt = { __index = _M }


function _M.new(self, broker_list)
    return setmetatable({
        correlation_id = 1,
        client_id = "client",
        request_timeout = 2000,
        broker_list = broker_list,
        topic_partitions = {},
        broker_nodes = {},
        brokers = {},
    }, mt)
end


function _M.close(self)
    local brokers = self.brokers

    for i = 1, #brokers do
        brokers[i]:close()
    end
end


local function correlation_id(self)
    local id = (self.correlation_id + 1) % 1073741824 -- 2^30
    self.correlation_id = id

    return id
end


local function produce_encode(self, topic, partition, messages)
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

    -- XX hard code for partition num
    req:int32(1)
    req:int32(partition)

    -- MessageSetSize and MessageSet
    req:message_set(messages)

    return req:package()
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

    return req:package()
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


local function fetch_metadata(self, topic, force_refresh)
    local partitions = self.topic_partitions[topic]
    if not force_refresh and partitions then
        return partitions
    end

    local broker_list = self.broker_list

    for i, config in ipairs(broker_list) do
        local bk, err = broker:new(config[1], config[2])

        if not bk then
            ngx_log(ngx_info, "broker connect failed, err:", err, config[1], config[2])
        else
            local frame = metadata_encode(self, topic)

            local resp, err = bk:send_receive(frame)
            bk:set_keepalive()

            if not resp then
                ngx_log(ngx_info, "broker metadata failed, err:", err, config[1], config[2])

            else
                local meta = metadata_decode(resp)

                local info = meta.topics[topic]

                self.topic_partitions[topic] = info
                self.broker_nodes = meta.brokers

                return info
            end
        end
    end

    return nil, "refresh metadata failed"
end


local function choose_partition(self, topic)
    local partitions = fetch_metadata(self, topic)

    local partition = partitions.partitions[self.correlation_id % partitions.num + 1]

    local id = partition.id
    local leader = partition.leader

    local bk = self.brokers[id]
    if bk then
        return bk, id
    end

    local config = self.broker_nodes[leader]

    local bk, err = broker:new(config.host, config.port)
    if not bk then
        return nil, err
    end

    return bk, id
end


function _M.send(self, topic, messages)
    local broker, partition = choose_partition(self, topic)

    if not broker then
        return nil, partition
    end

    local frame = produce_encode(self, topic, partition, messages)

    local resp, err = broker:send_receive(frame)

    if not resp then
        return nil, err
    end

    return produce_decode(resp)
end


return _M
