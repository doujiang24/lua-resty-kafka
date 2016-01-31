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

            local p = producer:new(broker_list, { producer_type = "async", flush_time = 10000 })
            ngx.sleep(0.1) -- will have an immediately flush by timer_flush

            local ok, err = p:send("test", key, message)
            if not ok then
                ngx.say("send err:", err)
                return
            end
            ngx.say("send ok:", ok)

            p:flush()
            local offset0 = p:offset()

            local ok, err = p:send("test", key, message)
            if not ok then
                ngx.say("send err:", err)
                return
            end
            ngx.say("send ok:", ok)

            p:flush()
            local offset1 = p:offset()

            ngx.say("send num:", tonumber(offset1 - offset0))
        ';
    }
--- request
GET /t
--- response_body
send ok:true
send ok:true
send num:1
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
            ngx.sleep(0.1) -- will have an immediately flush by timer_flush

            local size, err = p:send("test", key, message)
            if not size then
                ngx.say("send err:", err)
                return
            end

            ngx.sleep(1.1)

            local offset = p:offset()
            ngx.say("offset bigger than 0: ", tonumber(offset) > 0)
        ';
    }
--- request
GET /t
--- response_body
offset bigger than 0: true
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

            local p = producer:new(broker_list, { producer_type = "async", batch_num = 1, flush_time = 10000})
            ngx.sleep(0.1) -- will have an immediately flush by timer_flush

            local ok, err = p:send("test", nil, message)
            if not ok then
                ngx.say("send err:", err)
                return
            end
            ngx.say("send ok:", ok)

            ngx.sleep(1)

            local offset0 = p:offset()
            local send_num = p:flush()
            local offset1 = p:offset()
            ngx.say("send num:", tonumber(offset1 - offset0))

        ';
    }
--- request
GET /t
--- response_body
send ok:true
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
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_ERR_PORT },
            }

            local key = "key"
            local message = "halo world"

            local error_handle = function (topic, partition_id, queue, index, err, retryable)
                ngx.log(ngx.ERR, "failed to send to kafka, topic: ", topic, "; partition_id: ", partition_id, "; retryable: ", retryable)
            end

            local p = producer:new(broker_list, { producer_type = "async", max_retry = 1, batch_num = 1, error_handle = error_handle })

            local ok, err = p:send("test", key, message)
            if not ok then
                ngx.say("send err:", err)
                return
            end

            ngx.say("send ok:", ok)

            p:flush()

        ';
    }
--- request
GET /t
--- response_body
send ok:true
--- error_log: failed to send to kafka, topic: test; partition_id: -1; retryable: true



=== TEST 5: wrong in error handle
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_ERR_PORT },
            }

            local key = "key"
            local message = "halo world"

            local error_handle = function (topic, partition_id, queue, index, err, retryable)
                local num = topic + 1
                return true
            end
            ngx.log(ngx.ERR, tostring(error_handle))

            local p = producer:new(broker_list, { producer_type = "async", max_retry = 1, batch_num = 1, error_handle = error_handle })

            local ok, err = p:send("test", key, message)
            if not ok then
                ngx.say("send err:", err)
                return
            end

            ngx.say("send ok:", ok)

            p:flush()

        ';
    }
--- request
GET /t
--- response_body
send ok:true
--- error_log: failed to callback error_handle



=== TEST 6: work in log phase
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            ngx.req.read_body();
            local body = ngx.req.get_body_data();
            ngx.say(body);
        ';

        log_by_lua '
            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT},
            }

            local key = "key"
            local message = ngx.var.request_body

            local p = producer:new(broker_list, { producer_type = "async", batch_num = 1, flush_time = 10000})
            -- 1 message
            local size, err = p:send("test", key, message)

        ';
    }
--- request
POST /t
Hello world
--- response_body
Hello world
--- no_error_log
[error]



=== TEST 7: two topic in a batch
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            ngx.req.read_body();

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT},
            }

            local key = "key"
            local message = ngx.req.get_body_data();

            local p = producer:new(broker_list, { producer_type = "async", flush_time = 10000})
            ngx.sleep(0.01)
            -- 2 message
            local size, err = p:send("test", key, message)
            local size, err = p:send("test2", key, message)
            p:flush()
            local offset0 = p:offset()

            local size, err = p:send("test", key, message)
            local size, err = p:send("test2", key, message)
            p:flush()

            local offset1 = p:offset()

            ngx.say("send num:", tonumber(offset1 - offset0))
        ';
    }
--- request
POST /t
Hello world
--- response_body
send num:2
--- no_error_log
[error]



=== TEST 8: unretryable
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            ngx.req.read_body();

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT},
            }

            local key = "key"
            local message = ngx.req.get_body_data();

            local p = producer:new(broker_list, { producer_type = "async", flush_time = 10000})
            ngx.sleep(0.01)
            local size, err = p:send("test", key, message)
            p:flush()
            local offset0 = p:offset()

            -- XX: just hack for testing
            p.sendbuffer.topics.test[1].retryable = false

            local size, err = p:send("test", key, message)
            p:flush()

            local offset1 = p:offset()

            ngx.say("send num:", tonumber(offset1 - offset0))
        ';
    }
--- request
POST /t
Hello world
--- response_body
send num:1
--- no_error_log
[error]



=== TEST 9: two send in a batch
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            ngx.req.read_body();

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT},
            }

            local key = "key"
            local message = ngx.req.get_body_data();

            local p = producer:new(broker_list, { producer_type = "async", flush_time = 10000})
            ngx.sleep(0.01)
            -- 2 message
            local size, err = p:send("test", key, message)
            p:flush()
            local offset0 = p:offset()

            local size, err = p:send("test", key, message)
            local size, err = p:send("test", key, message)
            p:flush()

            local offset1 = p:offset()

            ngx.say("send num:", tonumber(offset1 - offset0))
        ';
    }
--- request
POST /t
Hello world
--- response_body
send num:2
--- no_error_log
[error]
