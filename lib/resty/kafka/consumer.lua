local request = require "resty.kafka.request"
local client = require "resty.kafka.client"
local utils = require "resty.kafka.utils"

local ffi = require "ffi"
local timer_every = ngx.timer.every
local ngx_log = ngx.log
local ngx_now = ngx.now
local ERR = ngx.ERR
local INFO = ngx.INFO
local DEBUG = ngx.DEBUG
local table_insert = require "table.insert"


local API_VERSION_V0  = 0
local API_VERSION_V1  = 1
local API_VERSION_V2  = 2
local API_VERSION_V3  = 3
local API_VERSION_V4  = 4
local API_VERSION_V5  = 5
local API_VERSION_V6  = 6
local API_VERSION_V7  = 7
local API_VERSION_V8  = 8
local API_VERSION_V9  = 9
local API_VERSION_V10 = 10
local API_VERSION_V11 = 11


local _M = { _VERSION = "0.10" }
local mt = { __index = _M }

-- 1. fetch_metadata
-- 2. fetch_offset
-- 3. fetch_message


-- weak value table is useless here, cause _timer_flush always ref p
-- so, weak value table won't works
local cluster_inited = {}
local DEFAULT_CLUSTER_NAME = 1


local function correlation_id(self)
    local id = utils.correlation_id(self.correlation_id)
    self.correlation_id = id

    return id
end


local function offset_fetch_encode(self, topics)
    local api_version = self.client:choose_api_version(request.OffsetRequest, API_VERSION_V0, API_VERSION_V2)
    local req = request:new(request.OffsetRequest,
                            correlation_id(self), self.client.client_id, api_version)

    req:int32(-1) -- replica_id

    if api_version >= API_VERSION_V2 then
        req:int8(self.isolation_level) -- isolation_level
    end

    req:int32(#topics)   -- [topics] array length

    for topic, topic_info in pairs(topics) do
        req:string(topic) -- [topics] name
        req:int32(#topic_info.partitions) -- [topics] [partitions] array length

        for _, partition_id in ipairs(topic_info.partitions) do
            req:int32(partition_id) -- [topics] [partitions] partition_index
            req:int64(ffi.new("int64_t", (ngx_now() * 1000))) -- [topics] [partitions] timestamp
        end
    end

    return req
end


local function offset_fetch_decode(resp)
    local ret = {}
    local api_version = resp.api_version

    local throttle_time_ms -- throttle_time_ms
    if api_version >= API_VERSION_V2 then
        throttle_time_ms = resp:int32()
    end

    local topic_num = resp:int32() -- [topics] array length
    
    for i = 1, topic_num do
        local topic = resp:string() -- [topics] name
        local partition_num = resp:int32() -- [topics] [partitions] array length

        ret[topic] = {}

        for j = 1, partition_num do
            local partition = resp:int32()

            if api_version == API_VERSION_V0 then
                ret[topic][partition] = {
                    errcode = resp:int16(),
                    offset = resp:int64(),
                }
            else
                ret[topic][partition] = {
                    errcode = resp:int16(),
                    timestamp = resp:int64(),
                    offset = resp:int64(),
                }
            end
        end
    end

    return ret, throttle_time_ms
end


local function fetch_encode(self, topics, topics_offset)
    local api_version = self.client:choose_api_version(request.FetchRequest, API_VERSION_V0, API_VERSION_V11)
    local req = request:new(request.FetchRequest,
                            correlation_id(self), self.client.client_id, api_version)

    req:int32(-1) -- replica_id
    req:int32(1000) -- max_wait_ms
    req:int32(0) -- min_bytes
    
    if api_version >= API_VERSION_V3 then
        req:int32(10 * 1024 * 1024) -- max_bytes: 10MB
    end

    if api_version >= API_VERSION_V4 then
        req:int8(self.isolation_level) -- isolation_level
    end

    if api_version >= API_VERSION_V7 then
        req:int32(0) -- session_id
        req:int32(-1) -- session_epoch
    end

    req:int32(#topics)   -- [topics] array length

    for topic, topic_info in pairs(topics) do
        req:string(topic) -- [topics] name
        req:int32(#topic_info.partitions) -- [topics] [partitions] array length

        for _, partition_id in ipairs(topic_info.partitions) do
            req:int32(partition_id) -- [topics] [partitions] partition

            if api_version >= API_VERSION_V9 then
                req:int32(-1) -- [topics] [partitions] current_leader_epoch
            end

            local topic_partition = topics_offset[topic][partition_id]
            if not topic_partition then
                req:int64(0) -- [topics] [partitions] fetch_offset
            else
                req:int64(topic_partition.offset) -- [topics] [partitions] fetch_offset
            end
            
            if api_version >= API_VERSION_V5 then
                req:int64(-1) -- [topics] [partitions] log_start_offset
            end

            req:int32(10 * 1024 * 1024) -- [topics] [partitions] partition_max_bytes
        end
    end

    if api_version >= API_VERSION_V7 then
        -- ForgottenTopics list add by KIP-227, only brokers use it, consumers do not use it
        req:int32(0) -- [forgotten_topics_data] array length
    end

    if api_version >= API_VERSION_V11 then
        req:string(self.client_rack) -- rack_id
    end

    return req
end


local function fetch_decode(resp)
    local ret = {}
    local api_version = resp.api_version

    local throttle_time_ms -- throttle_time_ms
    if api_version >= API_VERSION_V1 then
        throttle_time_ms = resp:int32() -- throttle_time_ms
    end

    local errcode, session_id
    if api_version >= API_VERSION_V7 then
        errcode = resp:int16() -- error_code
        session_id = resp:int32() -- session_id
    end

    local topic_num = resp:int32() -- [responses] array length

    for i = 1, topic_num do
        local topic = resp:string() -- [responses] topic

        local partition_num = resp:int32() -- [responses] [partitions] array length
        for j = 1, partition_num do
            local partition = resp:int32() -- [responses] [partitions] partition_index
            local errcode = resp:int16() -- [responses] [partitions] error_code
            local high_watermark = resp:int64() -- [responses] [partitions] high_watermark

            if api_version >= API_VERSION_V4 then
                local last_stable_offset = resp:int64() -- [responses] [partitions] last_stable_offset

                if api_version >= API_VERSION_V5 then
                    local log_start_offset = resp:int64() -- [responses] [partitions] log_start_offset
                end

                local aborted_transactions_num = resp:int32()
                for k = 1, aborted_transactions_num do
                    local producer_id = resp:int64() -- [responses] [partitions] [aborted_transactions] producer_id
                    local first_offset = resp:int64() -- [responses] [partitions] [aborted_transactions] first_offset
                end
            end

            if api_version > API_VERSION_V11 then
                local preferred_read_replica = resp:int32() -- [responses] [partitions] preferred_read_replica
            end

            local records = {} -- [responses] [partitions] records
        end
    end

    return ret, throttle_time_ms
end


local function _list_offset(self)
    local flat_topics = self.topics
    local topics = {}
    for _, value in pairs(t) do
        
    end

    local bk = self.client:choose_broker()
    local resp, err = bk.send_receive(offset_fetch_encode(client, self.topics))
    if not resp then
        -- error
    else
        self.offset = offset_fetch_decode(resp)
    end
end


local function _fetch_messages(self)
    if #self.topics <= 0 then
        return "no subscribed topic"
    end

    for _, topic in pairs(self.topics) do
        local client = self.client
        local bk = client:choose_broker()
        local resp, err = bk.send_receive(offset_fetch_encode(client, topic.topoc, topic.partition_id))
        if not resp then
            -- error
        else
            local offset = offset_fetch_decode(resp)
            local resp = bk.send_receive(fetch_encode(topic.topoc, topic.partition_id, offset))

            if not resp then
                -- error
            else
                local messages = fetch_decode(resp)

                if not self.buffer[topic.topoc] then
                    self.buffer[topic.topoc] = {}
                end

                if not self.buffer[topic.topoc][topic.partition_id] then
                    self.buffer[topic.topoc][topic.partition_id] = {}
                end

                for _, message in pairs(messages) do
                    table_insert(self.buffer[topic.topoc][topic.partition_id], message)
                end
            end
        end
    end
end


function _M.new(self, broker_list, consumer_config, cluster_name)
    local name = cluster_name or DEFAULT_CLUSTER_NAME
    local opts = consumer_config or {}
    local async = opts.consumer_type == "async"
    if async and cluster_inited[name] then
        return cluster_inited[name]
    end

    local cli = client:new(broker_list, consumer_config)
    local p = setmetatable({
        client = cli,
        correlation_id = 1,
        isolation_level = opts.isolation_level or 0,
        client_rack = opts.client_rack or "default",
        socket_config = cli.socket_config,
        topics = {},
        topics_metadata = {},
        topics_offset = {},
        buffer = {},
    }, mt)

    local ok, err = timer_every((opts.flush_time or 1000) / 1000, _fetch_messages, p) -- default: 1s
    if not ok then
        ngx_log(ERR, "failed to create timer_every, err: ", err)
    end

    return p
end


function _M.subscribe(self, topic, partition_id)
    if not self.topics[topic] then
        self.topics[topic] = {
            partitions = {partition_id},
        }
    else
        table_insert(self.topics[topic].partitions, partition_id)
    end
end


function _M.unsubscribe(self)
    self.topics = {}
end


function _M.poll(self)
    -- fetch topic metadata
    for _, topic in pairs(self.topics) do
        if not self.cli.topic_partitions[topic.topic] then
            self.cli.topic_partitions[topic.topic] = {}
        end
    end
    self.cli.refresh() -- force refresh metadata

    _list_offset(self)

    _fetch_messages(self)
end


return _M