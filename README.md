Name
=====

lua-resty-kafka - Lua kafka client driver for the ngx_lua based on the cosocket API

Table of Contents
=================

* [Name](#name)
* [Status](#status)
* [Description](#description)
* [Synopsis](#synopsis)
* [Modules](#modules)
    * [resty.kafka.client](#restykafkaclient)
        * [Methods](#methods)
            * [new](#new)
            * [fetch_metadata](#fetch_metadata)
            * [refresh](#refresh)
    * [resty.kafka.producer](#restykafkaproducer)
        * [Methods](#methods)
            * [new](#new)
            * [send](#send)
            * [offset](#offset)
            * [flush](#flush)
* [Installation](#installation)
* [TODO](#todo)
* [Author](#author)
* [Copyright and License](#copyright-and-license)
* [See Also](#see-also)

Status
======

This library is still under early development and is still experimental.

Description
===========

This Lua library is a Kafka client driver for the ngx_lua nginx module:

http://wiki.nginx.org/HttpLuaModule

This Lua library takes advantage of ngx_lua's cosocket API, which ensures
100% nonblocking behavior.

Note that at least [ngx_lua 0.9.3](https://github.com/openresty/lua-nginx-module/tags) or [ngx_openresty 1.4.3.7](http://openresty.org/#Download) is required, and unfortunately only LuaJIT supported (`--with-luajit`).


Synopsis
========

```lua
    lua_package_path "/path/to/lua-resty-kafka/lib/?.lua;;";

    server {
        location /test {
            content_by_lua '
                local cjson = require "cjson"
                local client = require "resty.kafka.client"
                local producer = require "resty.kafka.producer"

                local broker_list = {
                    { host = "127.0.0.1", port = 9092 },
                }

                local key = "key"
                local message = "halo world"

                -- usually we do not use this library directly
                local cli = client:new(broker_list)
                local brokers, partitions = cli:fetch_metadata("test")
                if not brokers then
                    ngx.say("fetch_metadata failed, err:", partitions)
                end
                ngx.say("brokers: ", cjson.encode(brokers), "; partitions: ", cjson.encode(partitions))


                -- sync producer_type
                local p = producer:new(broker_list)

                local offset, err = p:send("test", key, message)
                if not offset then
                    ngx.say("send err:", err)
                    return
                end
                ngx.say("send success, offset: ", tonumber(offset))

                -- this is async producer_type and bp will be reused in the whole nginx worker
                local bp = producer:new(broker_list, { producer_type = "async" })

                local ok, err = bp:send("test", key, message)
                if not ok then
                    ngx.say("send err:", err)
                    return
                end

                ngx.say("send success, ok:", ok)
            ';
        }
    }
```


[Back to TOC](#table-of-contents)

Modules
=======


resty.kafka.client
----------------------

To load this module, just do this

```lua
    local client = require "resty.kafka.client"
```

[Back to TOC](#table-of-contents)

### Methods

#### new

`syntax: c = client:new(broker_list, client_config)`

The `broker_list` is a list of broker, like the below

```json
[
    {
        "host": "127.0.0.1",
        "port": 9092
    }
]
```

An optional `client_config` table can be specified. The following options are as follows:

client config

* `socket_timeout`

    Specifies the network timeout threshold in milliseconds. *SHOULD* lagrer than the `request_timeout`.

* `keepalive_timeout`

    Specifies the maximal idle timeout (in milliseconds) for the keepalive connection.

* `keepalive_size`

    Specifies the maximal number of connections allowed in the connection pool for per Nginx worker.

* `refresh_interval`

    Specifies the time to auto refresh the metadata in milliseconds. Then metadata will not auto refresh if is nil.


[Back to TOC](#table-of-contents)

#### fetch_metadata
`syntax: brokers, partitions = c:fetch_metadata(topic)`

In case of success, return the all brokers and partitions of the `topic`.
In case of errors, returns `nil` with a string describing the error.


[Back to TOC](#table-of-contents)

#### refresh
`syntax: brokers, partitions = c:refresh()`

This will refresh the metadata of all topics which have been fetched by `fetch_metadata`.
In case of success, return all brokers and all partitions of all topics.
In case of errors, returns `nil` with a string describing the error.


[Back to TOC](#table-of-contents)

resty.kafka.producer
----------------------

To load this module, just do this

```lua
    local producer = require "resty.kafka.producer"
```

[Back to TOC](#table-of-contents)

### Methods

#### new

`syntax: p = producer:new(broker_list, producer_config?, cluster_name?)`

It's recommend to use async producer_type.

`broker_list` is the same as in `client`

An optional options table can be specified. The following options are as follows:

`socket_timeout`, `keepalive_timeout`, `keepalive_size`, `refresh_interval` are the same as in `client_config`

producer config, most like in <http://kafka.apache.org/documentation.html#producerconfigs>

* `producer_type`

    Specifies the `producer.type`. "async" or "sync"

* `request_timeout`

    Specifies the `request.timeout.ms`. Default `2000 ms`

* `required_acks`

    Specifies the `request.required.acks`, *SHOULD NOT* be zero. Default `1`.

* `max_retry`

    Specifies the `message.send.max.retries`. Default `3`.

* `retry_backoff`

    Specifies the `retry.backoff.ms`. Default `100`.

* `partitioner`

    Specifies the partitioner that choose partition from key and partition num.
    `syntax: partitioner = function (key, partition_num, correlation_id) end`,
    the correlation_id is an auto increment id in producer. Default partitioner is:


```lua
local function default_partitioner(key, num, correlation_id)
    local id = key and crc32(key) or correlation_id

    -- partition_id is continuous and start from 0
    return id % num
end
```

buffer config ( only work `producer_type` = "async" )

* `flush_time`

    Specifies the `queue.buffering.max.ms`. Default `1000`.

* `batch_num`

    Specifies the `batch.num.messages`. Default `200`.

* `batch_size`

    Specifies the `send.buffer.bytes`. Default `1M`(may reach 2M).
    Be careful, *SHOULD* be smaller than the `socket.request.max.bytes / 2 - 10k` config in kafka server.

* `max_buffering`

    Specifies the `queue.buffering.max.messages`. Default `50,000`.

* `error_handle`

    Specifies the error handle, handle data when buffer send to kafka error.
    `syntax: error_handle = function (topic, partition_id, message_queue, index, err, retryable) end`,
    the failed messages in the message_queue is like ```{ key1, msg1, key2, msg2 } ```,
    `key` in the message_queue is empty string `""` even if orign is `nil`.
    `index` is the message_queue length, should not use `#message_queue`.
    when `retryable` is `true` that means kafka server surely not committed this messages, you can safely retry to send;
    and else means maybe, recommend to log to somewhere.

Not support compression now.

The third optional `cluster_name` specifies the name of the cluster, default `1` (yeah, it's number). You can Specifies different names when you have two or more kafka clusters. And this only works with `async` producer_type.


[Back to TOC](#table-of-contents)

#### send
`syntax: ok, err = p:send(topic, key, message)`

1. In sync model

    In case of success, returns the offset (** cdata: LL **) of the current broker and partition.
    In case of errors, returns `nil` with a string describing the error.

2. In async model

    The `message` will write to the buffer first.
    It will send to the kafka server when the buffer exceed the `batch_num`,
    or every `flush_time` flush the buffer.

    It case of success, returns `true`.
    In case of errors, returns `nil` with a string describing the error (`buffer overflow`).


[Back to TOC](#table-of-contents)

#### offset

`syntax: sum, details = p:offset()`

    Return the sum of all the topic-partition offset (return by the ProduceRequest api);
    and the details of each topic-partition


[Back to TOC](#table-of-contents)

#### flush

`syntax: ok = p:flush()`

Always return `true`.


[Back to TOC](#table-of-contents)

Installation
============

You need to configure
the lua_package_path directive to add the path of your lua-resty-kafka source
tree to ngx_lua's LUA_PATH search path, as in

```nginx
    # nginx.conf
    http {
        lua_package_path "/path/to/lua-resty-kafka/lib/?.lua;;";
        ...
    }
```

Ensure that the system account running your Nginx ''worker'' proceses have
enough permission to read the `.lua` file.


[Back to TOC](#table-of-contents)

TODO
====

1.  Fetch API
2.  Offset API
3.  Offset Commit/Fetch API


[Back to TOC](#table-of-contents)

Author
======

Dejiang Zhu (doujiang24) <doujiang24@gmail.com>.


[Back to TOC](#table-of-contents)

Copyright and License
=====================

This module is licensed under the BSD license.

Copyright (C) 2014-2014, by Dejiang Zhu (doujiang24) <doujiang24@gmail.com>.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


[Back to TOC](#table-of-contents)

See Also
========
* the ngx_lua module: http://wiki.nginx.org/HttpLuaModule
* the kafka protocol: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
* the [lua-resty-redis](https://github.com/openresty/lua-resty-redis) library
* the [lua-resty-logger-socket](https://github.com/cloudflare/lua-resty-logger-socket) library
* the [sarama](https://github.com/Shopify/sarama)

[Back to TOC](#table-of-contents)

