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

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: set and get
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local producer = require "resty.kafka.producer"

            local broker_list = {
                { "$TEST_NGINX_KAFKA_HOST", $TEST_NGINX_KAFKA_PORT}
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

            ngx.say(cjson.encode(resp))

            local ok, err = p:close()
        ';
    }
--- request
GET /t
--- response_body_like
.*offset.*
--- no_error_log
[error]
