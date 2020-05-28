-- Copyright (C) Dejiang Zhu(doujiang24)


local broker = require "resty.kafka.broker"
local request = require "resty.kafka.request"


local setmetatable = setmetatable
local timer_at = ngx.timer.at
local ngx_log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local DEBUG = ngx.DEBUG
local debug = ngx.config.debug
local pid = ngx.worker.pid
local time = ngx.time
local sleep = ngx.sleep
local ceil = math.ceil
local pairs = pairs


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = { _VERSION = "0.09" }
local mt = { __index = _M }


local function _metadata_cache(self, topic)
    if not topic then
        return self.brokers, self.topic_partitions
    end

    local partitions = self.topic_partitions[topic]
    if partitions and partitions.num and partitions.num > 0 then
        return self.brokers, partitions
    end

    return nil, "not found topic"
end


local function metadata_encode(client_id, topics, num)
    local id = 0    -- hard code correlation_id
    local req = request:new(request.MetadataRequest, id, client_id)

    req:int32(num)

    for i = 1, num do
        req:string(topics[i])
    end

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
        local topic_info = new_tab(partition_num - 1, 3)

        topic_info.errcode = tp_errcode
        topic_info.num = partition_num

        for j = 1, partition_num do
            local partition_info = new_tab(0, 5)

            partition_info.errcode = resp:int16()
            partition_info.id = resp:int32()
            partition_info.leader = resp:int32()

            local repl_num = resp:int32()
            local replicas = new_tab(repl_num, 0)
            for m = 1, repl_num do
                replicas[m] = resp:int32()
            end
            partition_info.replicas = replicas

            local isr_num = resp:int32()
            local isr = new_tab(isr_num, 0)
            for m = 1, isr_num do
                isr[m] = resp:int32()
            end
            partition_info.isr = isr

            topic_info[partition_info.id] = partition_info
        end
        topics[topic] = topic_info
    end

    return brokers, topics
end


local function _fetch_metadata(self, new_topic)
    local topics, num = {}, 0
    for tp, _p in pairs(self.topic_partitions) do
        num = num + 1
        topics[num] = tp
    end

    if new_topic and not self.topic_partitions[new_topic] then
        num = num + 1
        topics[num] = new_topic
    end

    if num == 0 then
        return nil, "not topic"
    end

    local broker_list = self.broker_list
    local sc = self.socket_config
    local req = metadata_encode(self.client_id, topics, num)

    for i = 1, #broker_list do
        local host, port = broker_list[i].host, broker_list[i].port
        local bk = broker:new(host, port, sc)

        local resp, err = bk:send_receive(req)
        if not resp then
            ngx_log(INFO, "broker fetch metadata failed, err:", err,
                          ", host: ", host, ", port: ", port)
        else
            local brokers, topic_partitions = metadata_decode(resp)
            self.brokers, self.topic_partitions = brokers, topic_partitions

            return brokers, topic_partitions
        end
    end

    ngx_log(ERR, "all brokers failed in fetch topic metadata")
    return nil, "all brokers failed in fetch topic metadata"
end
_M.refresh = _fetch_metadata


local function meta_refresh(premature, self, interval)
    if premature then
        return
    end

    _fetch_metadata(self)

    local ok, err = timer_at(interval, meta_refresh, self, interval)
    if not ok then
        ngx_log(ERR, "failed to create timer at meta_refresh, err: ", err)
    end
end


function _M.new(self, broker_list, client_config)
    local opts = client_config or {}
    local socket_config = {
        socket_timeout = opts.socket_timeout or 3000,
        keepalive_timeout = opts.keepalive_timeout or 600 * 1000,   -- 10 min
        keepalive_size = opts.keepalive_size or 2,
        ssl = opts.ssl or false,
        ssl_verify = opts.ssl_verify or false
    }

    local cli = setmetatable({
        broker_list = broker_list,
        topic_partitions = {},
        brokers = {},
        client_id = "worker" .. pid(),
        socket_config = socket_config,
    }, mt)

    if opts.refresh_interval then
        meta_refresh(nil, cli, opts.refresh_interval / 1000) -- in ms
    end

    return cli
end


function _M.fetch_metadata(self, topic)
    local brokers, partitions = _metadata_cache(self, topic)
    if brokers then
        return brokers, partitions
    end

    _fetch_metadata(self, topic)

    return _metadata_cache(self, topic)
end


function _M.choose_broker(self, topic, partition_id)
    local brokers, partitions = self:fetch_metadata(topic)
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

    return config
end


return _M
