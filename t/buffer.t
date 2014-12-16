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


=== TEST 1: force flush
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local key = "key"
            local message = "halo world"

            local p = producer:new(broker_list, { producer_type = "async" })

            local size, err = p:send("test", key, message)
            if not size then
                ngx.say("send err:", err)
                return
            end

            ngx.say("send size:", size)

            local send_num = p:flush()
            ngx.say("send num:", send_num)

            local send_num = p:flush()
            ngx.say("send num:", send_num)
        ';
    }
--- request
GET /t
--- response_body
send size:13
send num:1
send num:0
--- no_error_log
[error]


=== TEST 2: timer flush
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local key = "key"
            local message = "halo world"

            local p = producer:new(broker_list, { producer_type = "async", flush_time = 1000 })

            local size, err = p:send("test", key, message)
            if not size then
                ngx.say("send err:", err)
                return
            end

            ngx.sleep(1.1)

            local send_num = p:flush()
            ngx.say("send num:", send_num)
        ';
    }
--- request
GET /t
--- response_body
send num:0
--- no_error_log
[error]


=== TEST 3: buffer flush
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local key = "key"
            local message = "halo world"

            local p = producer:new(broker_list, { producer_type = "async", flush_size = 1, flush_time = 1000})

            local size, err = p:send("test", nil, message)
            if not size then
                ngx.say("send err:", err)
                return
            end
            ngx.say("send size:", size)

            local size, err = p:send("test", key, message)
            ngx.say("send size:", size)

            ngx.sleep(0.5)

            local send_num = p:flush()
            ngx.say("send num:", send_num)

        ';
    }
--- request
GET /t
--- response_body
send size:10
send size:13
send num:0
--- no_error_log
[error]


=== TEST 4: error handle
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local key = "key"
            local message = "halo world"

            local error_handle = function (topic, partition_id, queue, index)
                ngx.log(ngx.ERR, "failed to send to kafka, topic: ", topic, "; partition_id: ", partition_id)
            end

            local p = producer:new(broker_list, { producer_type = "async", max_retry = 1, flush_size = 1, error_handle = error_handle })

            local size, err = p:send("test", key, message)
            if not size then
                ngx.say("send err:", err)
                return
            end

            -- just hack for test
            p.client.brokers = { [0] = { host = "127.0.0.1", port = 8080 } }

            ngx.sleep(0.5)
            ngx.say("send size:", size)
        ';
    }
--- request
GET /t
--- response_body
send size:13
--- error_log: failed to send to kafka, topic: test; partition_id: 1


=== TEST 5: buffer reuse
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local key = "key"
            local message = "halo world"

            local p0 = producer:new(broker_list)
            local offset1, err = p0:send("test", key, message)

            local p = producer:new(broker_list, { producer_type = "async" })

            -- 2 message
            local size, err = p:send("test", key, message)
            local size, err = p:send("test", key, message)
            local send_num = p:flush()

            -- 1 message
            local size, err = p:send("test", key, message)
            local send_num = p:flush()

            -- 1 message
            local size, err = p:send("test", key, message)
            local send_num = p:flush()

            local offset2, err = p0:send("test", key, message)

            ngx.say("offset diff: ", offset2 - offset1)
        ';
    }
--- request
GET /t
--- response_body
offset diff: 5
--- no_error_log
[error]
