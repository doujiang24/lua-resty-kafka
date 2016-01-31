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

=== TEST 1: add
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local sendbuffer = require "resty.kafka.sendbuffer"
            local buffer = sendbuffer:new(2, 20)

            local topic = "test"
            local partition_id = 1
            local key = "key"
            local message = "halo world"

            local overflow = buffer:add(topic, partition_id, key, message)
            ngx.say("overflow:", overflow)

            local overflow = buffer:add(topic, partition_id, key, message)
            ngx.say("overflow:", overflow)
        ';
    }
--- request
GET /t
--- response_body
overflow:nil
overflow:true
--- no_error_log
[error]



=== TEST 2: offset
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local sendbuffer = require "resty.kafka.sendbuffer"
            local buffer = sendbuffer:new(2, 20)

            local topic = "test"
            local partition_id = 1
            local key = "key"
            local message = "halo world"

            local overflow = buffer:add(topic, partition_id, key, message)
            ngx.say("overflow:", overflow)

            local offset = buffer:offset(topic, partition_id)
            ngx.say("offset:", offset)

            local offset = buffer:offset(topic, partition_id, 100)

            local offset = buffer:offset(topic, partition_id)
            ngx.say("offset:", offset)
        ';
    }
--- request
GET /t
--- response_body
overflow:nil
offset:0
offset:101
--- no_error_log
[error]



=== TEST 3: clear
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local sendbuffer = require "resty.kafka.sendbuffer"
            local buffer = sendbuffer:new(2, 20)

            local topic = "test"
            local partition_id = 1
            local key = "key"
            local message = "halo world"

            local overflow = buffer:add(topic, partition_id, key, message)
            ngx.say("overflow:", overflow)

            ngx.say("used:", buffer.topics[topic][partition_id].used)

            ngx.say("queue_num:", buffer.queue_num)

            buffer:clear(topic, partition_id)

            ngx.say("done:", buffer:done())

            ngx.say("queue_num:", buffer.queue_num)

            for i = 1, 10000 do
                buffer:clear(topic, partition_id)
            end

            ngx.say("used:", buffer.topics[topic][partition_id].used)
        ';
    }
--- request
GET /t
--- response_body
overflow:nil
used:0
queue_num:1
done:true
queue_num:0
used:1
--- no_error_log
[error]



=== TEST 4: loop
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local sendbuffer = require "resty.kafka.sendbuffer"
            local buffer = sendbuffer:new(2, 20)

            local topic = "test"
            local partition_id = 1
            local key = "key"
            local message = "halo world"

            local overflow = buffer:add(topic, partition_id, key, message)
            local overflow = buffer:add("test2", partition_id, key, message)

            for t, p in buffer:loop() do
                ngx.say("topic:", t, "; partition_id:", p)
            end
        ';
    }
--- request
GET /t
--- response_body
topic:test; partition_id:1
topic:test2; partition_id:1
--- no_error_log
[error]



=== TEST 5: aggregator
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local sendbuffer = require "resty.kafka.sendbuffer"
            local client = require "resty.kafka.client"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local cli = client:new(broker_list)

            local buffer = sendbuffer:new(2, 20)

            local topic = "test"
            local partition_id = 1
            local key = "key"
            local message = "halo world"

            cli:fetch_metadata(topic)
            cli:fetch_metadata("test2")
            cli:fetch_metadata("test3")
            cli:fetch_metadata("test4")
            cli:fetch_metadata("test5")

            local overflow = buffer:add(topic, partition_id, key, message)
            local overflow = buffer:add("test2", partition_id, key, message)
            local overflow = buffer:add("test3", partition_id, key, message)
            local overflow = buffer:add("test4", partition_id, key, message)
            local overflow = buffer:add("test5", partition_id, key, message)

            local num, sendbroker = buffer:aggregator(cli)
            ngx.say("num:", num/2)

            buffer:err("test5", partition_id, "timeout", false)
            buffer:err("test4", partition_id, "timeout", false)

            local num, sendbroker = buffer:aggregator(cli)
            ngx.say("num:", num/2)

            buffer:clear("test3", partition_id)
            buffer:clear("test2", partition_id)

            local num, sendbroker = buffer:aggregator(cli)
            ngx.say("num:", num/2)

            for t, p in buffer:loop() do
                ngx.say("topic:", t, "; partition_id:", p)
            end
        ';
    }
--- request
GET /t
--- response_body
num:3
num:2
num:1
topic:test5; partition_id:1
topic:test4; partition_id:1
topic:test; partition_id:1
--- no_error_log
[error]
