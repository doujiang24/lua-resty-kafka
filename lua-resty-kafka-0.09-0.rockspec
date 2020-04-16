package = "lua-resty-kafka"
version = "0.09-0"
source = {
   url = "git://github.com/doujiang24/lua-resty-kafka",
   tag = "v0.09"
}
description = {
   summary = "Lua Kafka client driver for the ngx_lua based on the cosocket API",
   detailed = [[
     This Lua library is a Kafka client driver for the ngx_lua nginx module:

     http://wiki.nginx.org/HttpLuaModule

     This Lua library takes advantage of ngx_lua's cosocket API, which ensures 100% nonblocking behavior.

     Note that at least ngx_lua 0.9.3 or ngx_openresty 1.4.3.7 is required, and unfortunately only LuaJIT supported (--with-luajit).
   ]],
   homepage = "https://github.com/doujiang24/lua-resty-kafka",
   license = "BSD"
}
dependencies = {
   "lua >= 5.1"
}
build = {
   type = "builtin",
   modules = {
      ["resty.kafka.broker"] = "lib/resty/kafka/broker.lua",
      ["resty.kafka.client"] = "lib/resty/kafka/client.lua",
      ["resty.kafka.errors"] = "lib/resty/kafka/errors.lua",
      ["resty.kafka.producer"] = "lib/resty/kafka/producer.lua",
      ["resty.kafka.request"] = "lib/resty/kafka/request.lua",
      ["resty.kafka.response"] = "lib/resty/kafka/response.lua",
      ["resty.kafka.ringbuffer"] = "lib/resty/kafka/ringbuffer.lua",
      ["resty.kafka.sendbuffer"] = "lib/resty/kafka/sendbuffer.lua"
   }
}
