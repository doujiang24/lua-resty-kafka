# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_KAFKA_HOST} = '127.0.0.1';
$ENV{TEST_NGINX_KAFKA_PORT} = '9092';
$ENV{TEST_NGINX_KAFKA_ERR_PORT} = '9091';

no_long_string();
#no_diff();

run_tests();

__DATA__


=== TEST 1: force flush
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p = producer:new(broker_list)

            local resp, err = p:send("test", messages)
            if not resp then
                ngx.say("send err:", err)
                return
            end

            ngx.say("buffer len: ", #p.buffers.test.accept_buffer.data)

            p:flush()

            ngx.say("buffer len: ", #p.buffers.test.accept_buffer.data)
        ';
    }
--- request
GET /t
--- response_body
buffer len: 1
buffer len: 0
--- no_error_log
[error]


=== TEST 2: timer flush
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p = producer:new(broker_list, { flush_time = 1})

            local resp, err = p:send("test", messages)
            if not resp then
                ngx.say("send err:", err)
                return
            end

            ngx.say("buffer len: ", #p.buffers.test.accept_buffer.data)

            ngx.sleep(1.1)

            ngx.say("buffer len: ", #p.buffers.test.accept_buffer.data)
        ';
    }
--- request
GET /t
--- response_body
buffer len: 1
buffer len: 0
--- no_error_log
[error]


=== TEST 3: buffer flush
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p = producer:new(broker_list, { flush_length = 2})

            local resp, err = p:send("test", messages)
            if not resp then
                ngx.say("send err:", err)
                return
            end

            ngx.say("buffer len: ", p.buffers.test.accept_buffer.index)

            local resp, err = p:send("test", messages)
            ngx.say("buffer len: ", p.buffers.test.accept_buffer.index)

            ngx.sleep(0.5)
            ngx.say("buffer len: ", p.buffers.test.accept_buffer.index)

        ';
    }
--- request
GET /t
--- response_body
buffer len: 1
buffer len: 2
buffer len: 0
--- no_error_log
[error]


=== TEST 4: error handle
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_ERR_PORT },
            }

            local messages = {
                "halo world",
            }

            local error_handle = function (topic, messages, index)
                ngx.log(ngx.ERR, "failed to send to kafka, topic: ", topic)
            end

            local p = producer:new(broker_list, { flush_length = 1, error_handle = error_handle })

            local resp, err = p:send("test", messages)
            if not resp then
                ngx.say("send err:", err)
                return
            end

            ngx.sleep(1)
            ngx.say("nothing to say")
        ';
    }
--- request
GET /t
--- response_body
nothing to say
--- error_log: failed to send to kafka, topic: test
