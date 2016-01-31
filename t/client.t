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

=== TEST 1: simple fetch
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local client = require "resty.kafka.client"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local cli = client:new(broker_list)

            local brokers, partitions = cli:fetch_metadata("test")
            if not brokers then
                ngx.say("fetch err:", partitions)
                return
            end

            ngx.say(cjson.encode(partitions))
        ';
    }
--- request
GET /t
--- response_body_like
.*replicas.*
--- no_error_log
[error]



=== TEST 2: timer refresh
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local client = require "resty.kafka.client"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local cli = client:new(broker_list, { refresh_interval =  100 })
            -- XXX just hack for test
            cli.topic_partitions = { test = {}, test1 = {} }

            ngx.sleep(0.5)

            ngx.say(cjson.encode(cli.topic_partitions))
        ';
    }
--- request
GET /t
--- response_body_like
.*replicas.*
--- no_error_log
[error]
