local protocol = require "resty.kafka.protocol.common"
local proto_record = require "resty.kafka.protocol.record"
local request = require "resty.kafka.request"

local ffi = require "ffi"
local ngx_now = ngx.now
local table_insert = table.insert

local _M = {}


local function _offset_fetch_encode(req, isolation_level, topic_partitions, client_rack)
    req:int32(-1) -- replica_id

    if req.api_version >= protocol.API_VERSION_V2 then
        req:int8(isolation_level) -- isolation_level
    end

    req:int32(#topic_partitions)   -- [topics] array length

    for topic, topic_partition in pairs(topic_partitions) do
        req:string(topic) -- [topics] name
        req:int32(#topic_partition) -- [topics] [partitions] array length

        for partition in pairs(topic_partition) do
            req:int32(partition) -- [topics] [partitions] partition_index
            req:int64(ffi.new("int64_t", (ngx_now() * 1000))) -- [topics] [partitions] timestamp
        end
    end

    return req
end


local function _fetch_encode(req, isolation_level, topic_partitions, rack_id)
    req:int32(-1) -- replica_id
    req:int32(1000) -- max_wait_ms
    req:int32(0) -- min_bytes
    
    if req.api_version >= protocol.API_VERSION_V3 then
        req:int32(10 * 1024 * 1024) -- max_bytes: 10MB
    end

    if req.api_version >= protocol.API_VERSION_V4 then
        req:int8(isolation_level) -- isolation_level
    end

    if req.api_version >= protocol.API_VERSION_V7 then
        req:int32(0) -- session_id
        req:int32(-1) -- session_epoch
    end

    req:int32(topic_partitions.topic_num)   -- [topics] array length

    for topic, partitions in pairs(topic_partitions.topics) do
        req:string(topic) -- [topics] name
        req:int32(partitions.partition_num) -- [topics] [partitions] array length

        for partition_id, partition_info in pairs(partitions.partitions) do
            req:int32(partition_id) -- [topics] [partitions] partition

            if req.api_version >= protocol.API_VERSION_V9 then
                req:int32(-1) -- [topics] [partitions] current_leader_epoch
            end

            req:int64(partition_info.offset) -- [topics] [partitions] fetch_offset

            if req.api_version >= protocol.API_VERSION_V5 then
                req:int64(-1) -- [topics] [partitions] log_start_offset
            end

            req:int32(10 * 1024 * 1024) -- [topics] [partitions] partition_max_bytes
        end
    end

    if req.api_version >= protocol.API_VERSION_V7 then
        -- ForgottenTopics list add by KIP-227, only brokers use it, consumers do not use it
        req:int32(0) -- [forgotten_topics_data] array length
    end

    if req.api_version >= protocol.API_VERSION_V11 then
        req:string(rack_id) -- rack_id
    end

    return req
end


function _M.offset_fetch_encode(consumer, topic_partitions, isolation_level)
    local client = consumer.client

    -- determine API version (min: v0; max: v2)
    local api_version = client:choose_api_version(protocol.OffsetRequest,
                                                  protocol.API_VERSION_V0,
                                                  protocol.API_VERSION_V2)

    local req = request:new(protocol.OffsetRequest, 
                            protocol.correlation_id(consumer),
                            client.client_id, api_version)

    return _offset_fetch_encode(req, isolation_level, topic_partitions)
end


function _M.offset_fetch_decode(resp)
    local ret = {}
    local api_version = resp.api_version

    local throttle_time_ms -- throttle_time_ms
    if api_version >= protocol.API_VERSION_V2 then
        throttle_time_ms = resp:int32()
    end

    local topic_num = resp:int32() -- [topics] array length
    
    for i = 1, topic_num do
        local topic = resp:string() -- [topics] name
        local partition_num = resp:int32() -- [topics] [partitions] array length

        ret[topic] = {}

        for j = 1, partition_num do
            local partition = resp:int32()

            if api_version == protocol.API_VERSION_V0 then
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


function _M.fetch_encode(consumer, topic_partitions, client_rack)
    local client = consumer.client

    -- determine API version (min: v0; max: v11)
    local api_version = client:choose_api_version(request.FetchRequest,
                                                       protocol.API_VERSION_V0,
                                                       protocol.API_VERSION_V11)

    local req = request:new(request.FetchRequest,
                            protocol.correlation_id(consumer),
                            client.client_id, api_version)

    return _fetch_encode(req, 0, topic_partitions, client_rack)
end


function _M.fetch_decode(resp)
    local fetch_info = {}
    local api_version = resp.api_version

    if api_version >= protocol.API_VERSION_V1 then
        fetch_info.throttle_time_ms = resp:int32() -- throttle_time_ms
    end

    if api_version >= protocol.API_VERSION_V7 then
        fetch_info.errcode = resp:int16() -- error_code
        fetch_info.session_id = resp:int32() -- session_id
    end

    local topic_num = resp:int32() -- [responses] array length

    local topic_partitions = {
        topic_num = topic_num,
        topics = {},
    }

    for i = 1, topic_num do
        local topic = resp:string() -- [responses] topic
        local partition_num = resp:int32() -- [responses] [partitions] array length

        topic_partitions.topics[topic] = {
            partition_num = partition_num,
            partitions = {}
        }

        for j = 1, partition_num do
            local partition = resp:int32() -- [responses] [partitions] partition_index

            local partition_ret = {
                errcode = resp:int16(), -- [responses] [partitions] error_code
                high_watermark = resp:int64(), -- [responses] [partitions] high_watermark
            }

            if api_version >= protocol.API_VERSION_V4 then
                partition_ret.last_stable_offset = resp:int64() -- [responses] [partitions] last_stable_offset

                if api_version >= protocol.API_VERSION_V5 then
                    partition_ret.log_start_offset = resp:int64() -- [responses] [partitions] log_start_offset
                end

                local aborted_transactions_num = resp:int32()
                partition_ret.aborted_transactions = {}
                for k = 1, aborted_transactions_num do
                    table_insert(partition_ret.aborted_transaction, {
                        producer_id = resp:int64(), -- [responses] [partitions] [aborted_transactions] producer_id
                        first_offset = resp:int64(), -- [responses] [partitions] [aborted_transactions] first_offset
                    })
                end
            end

            if api_version >= protocol.API_VERSION_V11 then
                partition_ret.preferred_read_replica = resp:int32() -- [responses] [partitions] preferred_read_replica
            end

            partition_ret.records = proto_record.message_set_decode(resp) -- [responses] [partitions] records

            topic_partitions.topics[topic].partitions[partition] = partition_ret
        end
    end

    return topic_partitions, fetch_info
end


return _M
