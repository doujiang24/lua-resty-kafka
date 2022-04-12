# vim:set ts=4 sw=4 et:

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
$ENV{TEST_NGINX_KAFKA_SSL_PORT} = '9093';
$ENV{TEST_NGINX_KAFKA_ERR_PORT} = '9091';
$ENV{TEST_NGINX_KAFKA_SASL_PORT} = '9094';
$ENV{TEST_NGINX_KAFKA_SASL_USER} = 'admin';
$ENV{TEST_NGINX_KAFKA_SASL_PWD} = 'admin-secret';


no_shuffle();
no_long_string();

run_tests();

__DATA__

=== TEST 1: send some test messages
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local message = "msg"

            local p = producer:new(broker_list)

            for i = 1, 135 do
                local offset, err = p:send("test-consumer", nil, message .. tostring(i))
                if not offset then
                    ngx.say("send err:", err)
                    return
                end
            end

            ngx.say("offset: ", tostring(offset))
        ';
    }
--- request
GET /t
--- response_body_like
.*offset.*
--- no_error_log
[error]



=== TEST 2: list offset (first)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            local offset0, err = c:list_offset("test-consumer", 0, protocol_consumer.LIST_OFFSET_TIMESTAMP_FIRST)
            if not offset0 then
                ngx.say(err)
                return
            end

            ngx.say("test-consumer: partition 0, offset: ", offset0.offset)

            local offset1, err = c:list_offset("test-consumer", 1, protocol_consumer.LIST_OFFSET_TIMESTAMP_FIRST)
            if not offset1 then
                ngx.say(err)
                return
            end

            ngx.say("test-consumer: partition 1, offset: ", offset1.offset)
        ';
    }
--- request
GET /t
--- response_body
test-consumer: partition 0, offset: 0LL
test-consumer: partition 1, offset: 0LL
--- no_error_log
[error]



=== TEST 3: list offset (last)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            ngx.sleep(1) -- wait 1 second for kafka
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            local offset0, err = c:list_offset("test-consumer", 0, protocol_consumer.LIST_OFFSET_TIMESTAMP_LAST)
            if not offset0 then
                ngx.say(err)
                return
            end

            ngx.say("test-consumer: partition 0, offset: ", offset0.offset)

            local offset1, err = c:list_offset("test-consumer", 1, protocol_consumer.LIST_OFFSET_TIMESTAMP_LAST)
            if not offset1 then
                ngx.say(err)
                return
            end

            ngx.say("test-consumer: partition 1, offset: ", offset1.offset)
        ';
    }
--- request
GET /t
--- response_body
test-consumer: partition 0, offset: 67LL
test-consumer: partition 1, offset: 68LL
--- no_error_log
[error]
