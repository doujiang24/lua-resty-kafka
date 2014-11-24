-- Copyright (C) Dejiang Zhu(doujiang24)


local broker = require "resty.kafka.broker"
local request = require "resty.kafka.request"


local setmetatable = setmetatable
local timer_at = ngx.timer.at
local ngx_log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local pid = ngx.worker.pid


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 4)
_M._VERSION = '0.01'


local mt = { __index = _M }


local function metadata_encode(self, topics)
    local client_id = self.client_id
    local id = 0    -- hard code correlation_id

    local req = request:new(request.MetadataRequest, id, client_id)

    local num = #topics
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

            topic_info.partitions[j] = partition_info
        end
        topics[topic] = topic_info
    end

    return brokers, topics
end


local function _fetch_metadata(self)
    local broker_list = self.broker_list
    local topics = self.topics
    local sc = self.socket_config
    local req = metadata_encode(self, topics)

    for i = 1, #broker_list do
        local host, port = broker_list[i].host, broker_list[i].port
        local bk = broker:new(host, port, sc)

        local resp, err = bk:send_receive(req)
        if not resp then
            ngx_log(INFO, "broker fetch metadata failed, err:", err, host, port)
        else
            local brokers, topic_partitions = metadata_decode(resp)
            self.broker_nodes, self.topic_partitions = brokers, topic_partitions

            return brokers, topic_partitions
        end
    end

    ngx_log(ERR, "refresh metadata failed")
    return nil, "refresh metadata failed"
end
_M.refresh = _fetch_metadata


local function meta_refresh(premature, self, interval)
    if premature then
        return
    end

    if #self.topics > 0 then
        _fetch_metadata(self)
    end

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
    }

    local cli = setmetatable({
        broker_list = broker_list,
        topic_partitions = {},
        broker_nodes = {},
        topics = {},
        client_id = "worker:" .. pid(),
        socket_config = socket_config,
    }, mt)

    if opts.refresh_interval then
        meta_refresh(nil, cli, opts.refresh_interval / 1000) -- in ms
    end

    return cli
end


function _M.fetch_metadata(self, topic)
    if not topic then
        return self.broker_nodes, self.topic_partitions
    end

    local partitions = self.topic_partitions[topic]
    if partitions then
        if partitions.num and partitions.num > 0 then
            return self.broker_nodes, partitions
        end
    else
        self.topics[#self.topics + 1] = topic
    end

    _fetch_metadata(self)

    local partitions = self.topic_partitions[topic]
    if partitions and partitions.num and partitions.num > 0 then
        return self.broker_nodes, partitions
    end

    return nil, "not found topic"
end


return _M
