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

=== TEST 1: simple pack
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local request = require "resty.kafka.request"
            local req = request:new(request.ProduceRequest, 1, "clientid")

            local function printx()
                local str = req._req[#req._req]
                for i = 1, #str do
                    ngx.print(bit.tohex(string.byte(str, i), 2))
                end
                ngx.say("")
            end

            req:int16(-1 * math.pow(2, 15)); printx()
            req:int16(math.pow(2, 15) - 1); printx()
            req:int16(-1); printx()
            req:int32(-1 * math.pow(2, 31)); printx()
            req:int32(math.pow(2, 31) - 1); printx()
            req:int64(-1LL * math.pow(2, 32) * math.pow(2, 31)); printx()
            req:int64(1ULL * math.pow(2, 32) * math.pow(2, 31) - 1); printx()
        ';
    }
--- request
GET /t
--- response_body
8000
7fff
ffff
80000000
7fffffff
8000000000000000
7fffffffffffffff
--- no_error_log
[error]



=== TEST 2: response unpack
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local request = require "resty.kafka.request"
            local response = require "resty.kafka.response"

            local function compare(func, number)
                local req = request:new(request.ProduceRequest, 1, "clientid")
                req:int32(100)
                local correlation_id = req._req[#req._req]

                req[func](req, number)
                local str = correlation_id .. req._req[#req._req]

                local resp = response:new(str)

                local cnumber = resp[func](resp)

                ngx.say(func, ": ", tostring(number), ", ", number == cnumber)
            end

            compare("int16", 0x7fff)
            compare("int16", 0x7fff * -1 - 1)
            compare("int32", 0x7fffffff)
            compare("int32", 0x7fffffff * -1 - 1)
            compare("int64", 1ULL * math.pow(2, 32) * math.pow(2, 31) - 1)
            compare("int64", -1LL * math.pow(2, 32) * math.pow(2, 31))
        ';
    }
--- request
GET /t
--- response_body
int16: 32767, true
int16: -32768, true
int32: 2147483647, true
int32: -2147483648, true
int64: 9223372036854775807ULL, true
int64: -9223372036854775808LL, true
--- no_error_log
[error]
