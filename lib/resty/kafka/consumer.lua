local client = require "resty.kafka.client"

local timer_every = ngx.timer.every
local ngx_log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local DEBUG = ngx.DEBUG
local table_insert = require "table.insert"


local _M = { _VERSION = "0.10" }
local mt = { __index = _M }

-- 1. fetch_metadata
-- 2. fetch_offset
-- 3. fetch_message


-- weak value table is useless here, cause _timer_flush always ref p
-- so, weak value table won't works
local cluster_inited = {}
local DEFAULT_CLUSTER_NAME = 1


local function offset_fetch_encode(topic, partition_id)
    
end


local function offset_fetch_decode(resp)
    
end


local function fetch_encode(topic, partition_id, offset)
    
end


local function fetch_decode(resp)
    
end


local function _fetch_messages(consumer)
    if #consumer.topics <= 0 then
        return "no subscribed topic"
    end

    for _, topic in pairs(consumer.topics) do
        local client = consumer.client
        local bk = client:choose_broker()
        local resp, err = bk.send_receive(offset_fetch_encode(topic.topoc, topic.partition_id))
        if not resp then
            -- error
        else
            local offset = offset_fetch_decode(resp)
            local resp = bk.send_receive(fetch_encode(topic.topoc, topic.partition_id, offset))

            if not resp then
                -- error
            else
                local messages = fetch_decode(resp)

                if not consumer.buffer[topic.topoc] then
                    consumer.buffer[topic.topoc] = {}
                end

                if not consumer.buffer[topic.topoc][topic.partition_id] then
                    consumer.buffer[topic.topoc][topic.partition_id] = {}
                end

                for _, message in pairs(messages) do
                    table_insert(consumer.buffer[topic.topoc][topic.partition_id], message)
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
        socket_config = cli.socket_config,
        topics = {},
        topics_metadata = {},
        buffer = {},
    }, mt)

    local ok, err = timer_every((opts.flush_time or 1000) / 1000, _fetch_messages, p) -- default: 1s
    if not ok then
        ngx_log(ERR, "failed to create timer_every, err: ", err)
    end

    return p
end


function _M.subscribe(self, topic, partition_id)
    table_insert(self.topics, {topic = topic, partition_id = partition_id})
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

    _fetch_messages(self)
end


return _M