# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

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


=== TEST 1: simple send
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p = producer:new(broker_list, { request_timeout = 1 })

            local offset, err = p:send("test", messages)
            if not offset then
                ngx.say("send err:", err)
                return
            end

            ngx.say("offset: ", offset)
        ';
    }
--- request
GET /t
--- response_body_like
.*offset.*
--- no_error_log
[error]


=== TEST 2: broker list has bad one
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_ERR_PORT },
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p, err = producer:new(broker_list)

            local offset, err = p:send("test", messages)
            if not offset then
                ngx.say("send err:", err)
                return
            end

            ngx.say("offset: ", offset)
        ';
    }
--- request
GET /t
--- response_body_like
.*offset.*
--- error_log: fetch_metadata


=== TEST 3: two topic send
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
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

            local resp, err = p:send("test1", messages)
            if not resp then
                ngx.say("send err:", err)
                return
            end

            -- hack use for test
            ngx.say(cjson.encode(p.client.topic_partitions["test"]))
        ';
    }
--- request
GET /t
--- response_body_like
.*partitions.*
--- no_error_log
[error]
