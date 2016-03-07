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
            local ringbuffer = require "resty.kafka.ringbuffer"
            local buffer = ringbuffer:new(2, 3)

            local topic = "test"
            local key = "key"
            local message = "halo world"

            local ok, err = buffer:add(topic, key, message)
            ngx.say("add ok:", ok, "; batch:", buffer:need_send())

            local ok, err = buffer:add(topic, key, message)
            ngx.say("add ok:", ok, "; batch:", buffer:need_send())

            local ok, err = buffer:add(topic, key, message)
            local ok, err = buffer:add(topic, key, message)
            if not ok then
                ngx.say("add err:", err)
                return
            end
            ngx.say("add ok:", ok, "; batch:", buffer:need_send())
        ';
    }
--- request
GET /t
--- response_body
add ok:true; batch:false
add ok:true; batch:true
add err:buffer overflow
--- no_error_log
[error]



=== TEST 2: pop
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local ringbuffer = require "resty.kafka.ringbuffer"
            local buffer = ringbuffer:new(2, 3)

            for i = 1, 2 do
                buffer:add("topic1", "key1", "message1")
                buffer:add("topic2", "key2", "message2")

                local topic, key, message = buffer:pop()
                ngx.say(topic, key, message)

                local topic, key, message = buffer:pop()
                ngx.say(topic, key, message)
            end

            local topic, key, message = buffer:pop()
            ngx.say(topic)
        ';
    }
--- request
GET /t
--- response_body
topic1key1message1
topic2key2message2
topic1key1message1
topic2key2message2
nil
--- no_error_log
[error]



=== TEST 3: left_num
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local ringbuffer = require "resty.kafka.ringbuffer"
            local buffer = ringbuffer:new(2, 3)

            buffer:add("topic1", "key1", "message1")
            buffer:add("topic2", "key2", "message2")
            buffer:add("topic2", "key2", "message2")

            local topic, key, message = buffer:pop()
            buffer:add("topic2", "key2", "message2")

            local num = buffer:left_num()
            ngx.say("num:", num)
        ';
    }
--- request
GET /t
--- response_body
num:3
--- no_error_log
[error]
