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
                --local offset, err = p:send("test-consumer", nil, message .. tostring(i))
                if not offset then
                    --ngx.say("send err:", err)
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



=== TEST 4: fetch message (first)
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
        ';
    }
--- request
GET /t
--- response_body
msg2msg4msg6msg8msg10msg12msg14msg16msg18msg20msg22msg24msg26msg28msg30msg32msg34msg36msg38msg40msg42msg44msg46msg48msg50msg52msg54msg56msg58msg60msg62msg64msg66msg68msg70msg72msg74msg76msg78msg80msg82msg84msg86msg88msg90msg92msg94msg96msg98msg100msg102msg104msg106msg108msg110msg112msg114msg116msg118msg120msg122msg124msg126msg128msg130msg132msg134
msg1msg3msg5msg7msg9msg11msg13msg15msg17msg19msg21msg23msg25msg27msg29msg31msg33msg35msg37msg39msg41msg43msg45msg47msg49msg51msg53msg55msg57msg59msg61msg63msg65msg67msg69msg71msg73msg75msg77msg79msg81msg83msg85msg87msg89msg91msg93msg95msg97msg99msg101msg103msg105msg107msg109msg111msg113msg115msg117msg119msg121msg123msg125msg127msg129msg131msg133msg135
--- no_error_log
[error]



=== TEST 5: fetch message (offset)
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
        ';
    }
--- request
GET /t
--- response_body
msg102msg104msg106msg108msg110msg112msg114msg116msg118msg120msg122msg124msg126msg128msg130msg132msg134
msg101msg103msg105msg107msg109msg111msg113msg115msg117msg119msg121msg123msg125msg127msg129msg131msg133msg135
--- no_error_log
[error]



=== TEST 6: fetch message (empty)
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

            local _, err = c:fetch("test-consumer", 0, 200) -- partition 0, offset 200
            if err == "OffsetOutOfRange" then
                ngx.say("OffsetOutOfRange0")
            end

            local _, err = c:fetch("test-consumer", 1, 200) -- partition 1, offset 200
            if err == "OffsetOutOfRange" then
                ngx.say("OffsetOutOfRange1")
            end
        ';
    }
--- request
GET /t
--- response_body
OffsetOutOfRange0
OffsetOutOfRange1
--- no_error_log
[error]
