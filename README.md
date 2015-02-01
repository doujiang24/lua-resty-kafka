lua-resty-kafka
===============

Lua kafka client driver for the ngx_lua based on the cosocket API

Status
======

This library is still under early development and is still experimental.

Description
===========

This Lua library is a Kafka client driver for the ngx_lua nginx module:

http://wiki.nginx.org/HttpLuaModule

This Lua library takes advantage of ngx_lua's cosocket API, which ensures
100% nonblocking behavior.

Note that at least [ngx_lua 0.9.3](https://github.com/openresty/lua-nginx-module/tags) or [ngx_openresty 1.4.3.7](http://openresty.org/#Download) is required.


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
                ngx.say("send success, offset: ", tostring(offset))

                -- this is async producer_type and bp will be reused in the whole nginx worker
                local bp = producer:new(broker_list, { producer_type = "async" })

                local size, err = p:send("test", key, message)
                if not size then
                    ngx.say("send err:", err)
                    return
                end

                ngx.say("send success, size", size)
            ';
        }
    }
```


Modules
=======


resty.kafka.client
----------------------

To load this module, just do this

```lua
    local client = require "resty.kafka.client"
```

### Methods

#### new

`syntax: p = producer:new(broker_list, client_config)`

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


#### fetch_metadata
`syntax: brokers, partitions = client:fetch_metadata(topic)`

In case of success, return the all brokers and partitions of the `topic`.
In case of errors, returns `nil` with a string describing the error.


#### refresh
`syntax: brokers, partitions = client:refresh()`

This will refresh the metadata of all topics which have been fetched by `fetch_metadata`.
In case of success, return all brokers and all partitions of all topics.
In case of errors, returns `nil` with a string describing the error.


resty.kafka.producer
----------------------

To load this module, just do this

```lua
    local producer = require "resty.kafka.producer"
```

### Methods

#### new

`syntax: p = producer:new(broker_list, producer_config)`

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

* `flush_size`

    Specifies the minimal buffer size(total byte size) to flush. Default 10240, 10KB.

* `flush_time`

    Specifies the time (in milliseconds) to flush. Default 1000ms.

* `max_size`

    Specifies the maximal buffer size to buffer. Default 10485760, 1MB.
    Be carefull, *SHOULD* be smaller than the `socket.request.max.bytes` config in kafka server.

* `error_handle`

    Specifies the error handle, handle data when buffer send to kafka error.
    `syntax: error_handle = function (topic, partition_id, message_queue, index, err) end`,
    the failed messages in the message_queue is like ```{ key1, msg1, key2, msg2 } ```,
    `key` in the message_queue is empty string `""` even if orign is `nil`.
    `index` is the message_queue length, should not use `#message_queue`.

Not support compression now.


#### send
`syntax: ok, err = p:send(topic, key, message)`

1. In sync model

    In case of success, returns the offset (** cdata: LL **) of the current broker and partition.
    In case of errors, returns `nil` with a string describing the error.

2. In async model

    The `message` will write to the buffer first.
    It will send to the kafka server when the buffer exceed the `flush_size`,
    or every `flush_time` flush the buffer.

    It case of success, returns the message size(byte) add to buffer (key length + message length).
    In case of errors, returns `nil` with a string describing the error (`buffer overflow` or `not found topic`).


#### flush

`syntax: num, err = bp:flush()`

It will force send the messages that buffered to kafka server.
Return the send success messages num.


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


TODO
====

1.  Fetch API
2.  Offset API
3.  Offset Commit/Fetch API


Author
======

Dejiang Zhu (doujiang24) <doujiang24@gmail.com>.


Copyright and License
=====================

This module is licensed under the BSD license.

Copyright (C) 2014-2014, by Dejiang Zhu (doujiang24) <doujiang24@gmail.com>.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


See Also
========
* the ngx_lua module: http://wiki.nginx.org/HttpLuaModule
* the kafka protocol: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
* the [lua-resty-redis](https://github.com/openresty/lua-resty-redis) library
* the [lua-resty-logger-socket](https://github.com/cloudflare/lua-resty-logger-socket) library
* the [Go implementation](https://github.com/Shopify/sarama)
