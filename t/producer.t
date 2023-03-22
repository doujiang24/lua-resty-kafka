# vim:set ts=4 sw=4 et:

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
$ENV{TEST_NGINX_KAFKA_SSL_PORT} = '9093';
$ENV{TEST_NGINX_KAFKA_ERR_PORT} = '9091';
$ENV{TEST_NGINX_KAFKA_SASL_PORT} = '9094';
$ENV{TEST_NGINX_KAFKA_SASL_USER} = 'admin';
$ENV{TEST_NGINX_KAFKA_SASL_PWD} = 'admin-secret';


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

            local message = "halo world"

            local p = producer:new(broker_list)

            local offset, err = p:send("test", nil, message)
            if not offset then
                ngx.say("send err:", err)
                return
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



=== TEST 2: simple ssl send
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SSL_PORT },
            }

            local message = "halo world"

            local p = producer:new(broker_list, { ssl = true })

            local offset, err = p:send("test", nil, message)
            if not offset then
                ngx.say("send err:", err)
                return
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



=== TEST 3: broker list has bad one
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

            local message = "halo world"

            local p, err = producer:new(broker_list)

            local offset, err = p:send("test", nil, message)
            if not offset then
                ngx.say("send err:", err)
                return
            end

            ngx.say("offset: ", tostring(offset))
        ';
    }
--- request
GET /t
--- response_body_like
.*offset.*
--- error_log: fetch_metadata



=== TEST 4: two send
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

            local p = producer:new(broker_list)

            local offset1, err = p:send("test", key, message)
            if not offset1 then
                ngx.say("send1 err:", err)
                return
            end

            local offset2, err = p:send("test", key, message)
            if not offset2 then
                ngx.say("send2 err:", err)
                return
            end

            ngx.say("offset diff: ", tonumber(offset2 - offset1))
        ';
    }
--- request
GET /t
--- response_body
offset diff: 1
--- no_error_log
[error]



=== TEST 5: two topic send
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

            local p = producer:new(broker_list)

            local offset1, err = p:send("test", key, message)
            if not offset1 then
                ngx.say("send1 err:", err)
                return
            end

            local offset2, err = p:send("test2", key, message)
            if not offset2 then
                ngx.say("send2 err:", err)
                return
            end

            ngx.say("two topic successed!")
        ';
    }
--- request
GET /t
--- response_body
two topic successed!
--- no_error_log
[error]



=== TEST 6: kafka return error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local message = "halo world"
            local p, err = producer:new(broker_list)

            local offset, err = p:send("test", "a", message)
            if not offset then
                ngx.say("send err:", err)
                return
            end

            -- XX: just hack for testing
            p.client.topic_partitions.test = { [2] = { id = 2, leader = 0 }, [1] = { id = 1, leader = 0 }, [0] = { id = 0, leader = 0 }, num = 3 }

            local offset2, err = p:send("test", "b", message)
            if not offset2 then
                ngx.say("send err:", err)
                return
            end
            ngx.say("offset: ", tostring(offset2 - offset))
        ';
    }
--- request
GET /t
--- response_body
send err:not found partition
--- no_error_log
[error]



=== TEST 7: add a lot of messages
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }
            local topic = "test"
            local key = "key"
            local message = "halo world"
            local p, err = producer:new(broker_list, { producer_type = "async", flush_time = 100})
            -- init offset
            p:send(topic, key, message)
            p:flush()
            local offset,_ = p:offset()
            local i = 0
            while i < 2000 do
                 p:send(topic, key, message..tostring(i))
                 i = i + 1
            end
            ngx.sleep(0.2)
            local offset2, _ = p:offset()
            ngx.say("offset: ", tostring(offset2 - offset))
        }
    }
--- request
GET /t
--- response_body
offset: 2000LL
--- no_error_log
[error]



=== TEST 8: sasl simple send
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SASL_PORT ,
                sasl_config = { mechanism="PLAIN", user="$TEST_NGINX_KAFKA_SASL_USER", password = "$TEST_NGINX_KAFKA_SASL_PWD" },},
            }

            local message = "halo world"

            local p = producer:new(broker_list)

            local offset, err = p:send("test", nil, message)
            if not offset then
                ngx.say("send err:", err)
                return
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

=== TEST 9: sasl SCRAM-SHA-256 simple send
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SASL_PORT ,
                sasl_config = { mechanism="SCRAM-SHA-256", user="$TEST_NGINX_KAFKA_SASL_USER", password = "$TEST_NGINX_KAFKA_SASL_PWD" },},
            }

            local message = "halo world"

            local p = producer:new(broker_list)

            local offset, err = p:send("test", nil, message)
            if not offset then
                ngx.say("send err:", err)
                return
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

=== TEST 10: sasl SCRAM-SHA-512 simple send
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SASL_PORT ,
                sasl_config = { mechanism="SCRAM-SHA-512", user="$TEST_NGINX_KAFKA_SASL_USER", password = "$TEST_NGINX_KAFKA_SASL_PWD" },},
            }

            local message = "halo world"

            local p = producer:new(broker_list)

            local offset, err = p:send("test", nil, message)
            if not offset then
                ngx.say("send err:", err)
                return
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
