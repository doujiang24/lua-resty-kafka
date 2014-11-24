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
            local bufferproducer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p = bufferproducer:new("cluster_1", broker_list)

            local ok, err = p:send("test", messages)
            if not ok then
                ngx.say("send err:", err)
                return
            end

            local send_num = p:flush()
            ngx.say("send num:", send_num)

            local send_num = p:flush()
            ngx.say("send num:", send_num)
        ';
    }
--- request
GET /t
--- response_body
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
            local bufferproducer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p = bufferproducer:new(nil, broker_list, nil, nil, { flush_time = 1000 })

            local ok, err = p:send("test", messages)
            if not ok then
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
            local bufferproducer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local p = bufferproducer:new(nil, broker_list, nil, nil, { flush_size = 1, flush_time = 1000})

            local ok, err = p:send("test", messages)
            if not ok then
                ngx.say("send err:", err)
                return
            end

            local ok, err = p:send("test", messages)

            ngx.sleep(0.5)

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


=== TEST 4: error handle
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local bufferproducer = require "resty.kafka.bufferproducer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_ERR_PORT },
            }

            local messages = {
                "halo world",
            }

            local error_handle = function (topic, messages, index)
                ngx.log(ngx.ERR, "failed to send to kafka, topic: ", topic)
            end

            local p = bufferproducer:new(nil, broker_list, nil, nil, { flush_size = 1, error_handle = error_handle })

            local ok, err = p:send("test", messages)
            if not ok then
                ngx.say("send err:", err)
                return
            end

            ngx.sleep(0.5)
            ngx.say("nothing to say")
        ';
    }
--- request
GET /t
--- response_body
nothing to say
--- error_log: failed to send to kafka, topic: test
