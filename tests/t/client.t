# vim:set ts=4 sw=4 et:

# use Test::Nginx::Socket::Lua;
use Test::Nginx::Socket::Lua 'no_plan';
use Cwd qw(cwd);

# repeat_each(1);

# plan tests => repeat_each();

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
};

$ENV{TEST_NGINX_RESOLVER} = '127.0.0.11';
$ENV{TEST_NGINX_KAFKA_HOST} = 'broker';
$ENV{TEST_NGINX_KAFKA_PORT} = '9092';
$ENV{TEST_NGINX_KAFKA_SSL_PORT} = '9093';
$ENV{TEST_NGINX_KAFKA_ERR_PORT} = '9091';

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: simple fetch
--- http_config eval: $::HttpConfig
--- config
    location /t {
        resolver 127.0.0.11;
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
        resolver 127.0.0.11;
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
        resolver 127.0.0.11;
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

=== TEST 4: simple mTLS fetch
--- http_config eval: $::HttpConfig
--- config
    location /t {
        resolver 127.0.0.11;
        content_by_lua_block {

            local cjson = require "cjson"
            local client = require "resty.kafka.client"
            local ssl = require "ngx.ssl"

            local broker_list = {
                { host = "$TEST_NGINX_KAFKA_HOST", port = $TEST_NGINX_KAFKA_SSL_PORT },
            }

            local f = assert(io.open("/certs/certchain.crt"))
            local cert_data = f:read("*a")
            f:close()

            local CERT, err = ssl.parse_pem_cert(cert_data)
            if not CERT then
                ngx_log(ERR, "error parsing cert: ", err)
                return nil, err
            end

            local f = assert(io.open("/certs/privkey.key"))
            local key_data = f:read("*a")
            f:close()

            local CERT_KEY, err = ssl.parse_pem_priv_key(key_data)
            if not CERT_KEY then
                ngx_log(ERR, "unable to parse cert key file: ", err)
                return nil, err
            end

            local cli = client:new(broker_list, {
                ssl = true,
                client_cert = CERT,
                client_priv_key = CERT_KEY,
            })

            local cli = client:new(broker_list, {
                ssl = true,
                client_cert = CERT,
                client_priv_key = CERT_KEY,
            })

            local brokers, partitions = cli:fetch_metadata("test")
            if not brokers then
                ngx.say("fetch err:", partitions)
                return
            end

            ngx.say(cjson.encode(partitions))
        } 
    }
--- request
GET /t
--- response_body_like
.*replicas.*
--- no_error_log
[error]