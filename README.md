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
                local producer = require "resty.kafka.producer"

                local broker_list = {
                    { host = "127.0.0.1", port = 9092 },
                }
                local messages = {
                    "halo world",
                }

                local p, err = producer:new(broker_list)

                local resp, err = p:send("test", messages)
                if not resp then
                    ngx.say("send err:", err)
                    return
                end

                ngx.say("send success, result: ", cjson.encode(resp))
            ';
        }
    }
```

or use the bufferproducer


```lua
    # you do not need the following line if you are using
    # the ngx_openresty bundle:
    lua_package_path "/path/to/lua-resty-kafka/lib/?.lua;;";

    server {
        location /test {
            content_by_lua '
                local cjson = require "cjson"
                local producer = require "resty.kafka.bufferproducer"

                local broker_list = {
                    { host = "127.0.0.1", port = 9092 },
                }

                local messages = {
                    "halo world",
                }

                local p = producer:init(broker_list, { flush_time = 1 })

                local resp, err = p:send("test", messages)
                if not resp then
                    ngx.say("send err:", err)
                    return
                end

                ngx.say("send success, result: ", cjson.encode(resp))
            ';
        }
    }
```

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

