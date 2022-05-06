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



=== TEST 2: simple ssl fetch
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local client = require "resty.kafka.client"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SSL_PORT },
            }

            local messages = {
                "halo world",
            }

            local cli = client:new(broker_list, { ssl = true})

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



=== TEST 3: timer refresh
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



=== TEST 4: simple fetch sasl
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local client = require "resty.kafka.client"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SASL_PORT ,
                  sasl_config = { mechanism="PLAIN", user="$TEST_NGINX_KAFKA_SASL_USER", password = "$TEST_NGINX_KAFKA_SASL_PWD" },},
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



=== TEST 5: timer refresh sasl
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local client = require "resty.kafka.client"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SASL_PORT ,
                sasl_config = { mechanism="PLAIN", user="$TEST_NGINX_KAFKA_SASL_USER", password = "$TEST_NGINX_KAFKA_SASL_PWD" },},
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



=== TEST 6: ApiVersions fetch
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
            
            ngx.say(cjson.encode(cli.api_versions))
        ';
    }
--- request
GET /t
--- response_body eval
qr/\"max_version\":/ and qr /\"min_version\":/
--- no_error_log
[error]



=== TEST 7: ApiVersions choose
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local cjson = require "cjson"
            local request = require "resty.kafka.request"
            local client = require "resty.kafka.client"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_PORT },
            }

            local messages = {
                "halo world",
            }

            local cli = client:new(broker_list)

            local brokers, partitions = cli:fetch_metadata("test")

            -- not input version range
            ngx.say(cli:choose_api_version(request.FetchRequest))

            -- not exist api_key
            ngx.say(cli:choose_api_version(-1))

            -- set max version to -1 to break version choose
            ngx.say(cli:choose_api_version(request.FetchRequest, 0, -1))

            -- set lower max version to limit the API version
            ngx.say(cli:choose_api_version(request.FetchRequest, 0, 5))
            
            -- set higher max version to use the highest API version supported by broker
            ngx.say(cli:choose_api_version(request.FetchRequest, 0, 9999))
        ';
    }
--- request
GET /t
--- response_body
11
-1
-1
5
11
--- no_error_log
[error]


=== TEST 8: fetch with resolving
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"
            local client = require "resty.kafka.client"

            local count = 0
            local function resolver(host)
                count = count + 1
                return "$TEST_NGINX_KAFKA_HOST"
            end
            local broker_list = {
                { host = "toresolve", port = $TEST_NGINX_KAFKA_PORT },
            }

            local cli = client:new(broker_list, { resolver = resolver })
            local brokers, partitions = cli:fetch_metadata("test")
            ngx.say("result ", count)
        ';
    }
--- request
GET /t
--- response_body_like
.*result [1-9].*
--- no_error_log
[error]