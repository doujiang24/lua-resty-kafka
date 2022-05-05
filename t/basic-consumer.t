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
        content_by_lua_block {
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local p = producer:new(broker_list)

            for i = 1, 135 do
                local offset, err = p:send("test-consumer", nil, tostring(i))
                if not offset then
                    ngx.say("send err:", err)
                    return
                end
            end

            ngx.say("offset: ", tostring(offset))
        }
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
        content_by_lua_block {

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

            ngx.say("test-consumer: partition 0, offset: ", offset0)

            local offset1, err = c:list_offset("test-consumer", 1, protocol_consumer.LIST_OFFSET_TIMESTAMP_FIRST)
            if not offset1 then
                ngx.say(err)
                return
            end

            ngx.say("test-consumer: partition 1, offset: ", offset1)
        }
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
        content_by_lua_block {
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

            ngx.say("test-consumer: partition 0, offset: ", offset0)

            local offset1, err = c:list_offset("test-consumer", 1, protocol_consumer.LIST_OFFSET_TIMESTAMP_LAST)
            if not offset1 then
                ngx.say(err)
                return
            end

            ngx.say("test-consumer: partition 1, offset: ", offset1)
        }
    }
--- request
GET /t
--- response_body
test-consumer: partition 0, offset: 67LL
test-consumer: partition 1, offset: 68LL
--- no_error_log
[error]



=== TEST 4: list offset (topic not exist)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            ngx.sleep(1) -- wait 1 second for kafka
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            -- It will return an error at the "choose broker" step, because the corresponding topic cannot be found
            local offset, err = c:list_offset("not-exist-topic", 0, protocol_consumer.LIST_OFFSET_TIMESTAMP_LAST)
            if not offset then
                ngx.say(err)
                return
            end
        }
    }
--- request
GET /t
--- response_body
not found topic
--- no_error_log
[error]



=== TEST 5: list offset (partition not exist)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            ngx.sleep(1) -- wait 1 second for kafka
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            -- It will return an error at the "choose broker" step, because the corresponding topic cannot be found
            local offset, err = c:list_offset("test-consumer", 999, protocol_consumer.LIST_OFFSET_TIMESTAMP_LAST)
            if not offset then
                ngx.say(err)
                return
            end
        }
    }
--- request
GET /t
--- response_body
not found partition
--- no_error_log
[error]



=== TEST 6: fetch message (first)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            local ret0, err = c:fetch("test-consumer", 0, 0) -- partition 0, offset 0
            local message0 = ""
            for _, record in pairs(ret0.records) do
                message0 = message0 .. record.value
            end
            ngx.say(message0)

            local ret1, err = c:fetch("test-consumer", 1, 0) -- partition 1, offset 0
            local message1 = ""
            for _, record in pairs(ret1.records) do
                message1 = message1 .. record.value
            end
            ngx.say(message1)
        }
    }
--- request
GET /t
--- response_body
2468101214161820222426283032343638404244464850525456586062646668707274767880828486889092949698100102104106108110112114116118120122124126128130132134
13579111315171921232527293133353739414345474951535557596163656769717375777981838587899193959799101103105107109111113115117119121123125127129131133135
--- no_error_log
[error]



=== TEST 7: fetch message (offset)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            local ret0, err = c:fetch("test-consumer", 0, 50) -- partition 0, offset 50
            local message0 = ""
            for _, record in pairs(ret0.records) do
                message0 = message0 .. record.value
            end
            ngx.say(message0)

            local ret1, err = c:fetch("test-consumer", 1, 50) -- partition 1, offset 50
            local message1 = ""
            for _, record in pairs(ret1.records) do
                message1 = message1 .. record.value
            end
            ngx.say(message1)
        }
    }
--- request
GET /t
--- response_body
102104106108110112114116118120122124126128130132134
101103105107109111113115117119121123125127129131133135
--- no_error_log
[error]



=== TEST 8: fetch message (empty)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            local _, err = c:fetch("test-consumer", 0, 200) -- partition 0, offset 200
            if err == "OFFSET_OUT_OF_RANGE" then
                ngx.say(err.."0")
            end

            local _, err = c:fetch("test-consumer", 1, 200) -- partition 1, offset 200
            if err == "OFFSET_OUT_OF_RANGE" then
                ngx.say(err.."1")
            end
        }
    }
--- request
GET /t
--- response_body
OFFSET_OUT_OF_RANGE0
OFFSET_OUT_OF_RANGE1
--- no_error_log
[error]



=== TEST 9: fetch message (topic not exist)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            local ret, err = c:fetch("not-exist-topic", 0, 0) -- partition 0, offset 0
            if not ret then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
not found topic
--- no_error_log
[error]



=== TEST 10: fetch message (partition not exist)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local cjson = require("cjson")
            local bconsumer = require("resty.kafka.basic-consumer")
            local protocol_consumer = require("resty.kafka.protocol.consumer")

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local c = bconsumer:new(broker_list)

            local ret, err = c:fetch("test-consumer", 999, 0) -- partition 999, offset 0
            if not ret then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
not found partition
--- no_error_log
[error]
