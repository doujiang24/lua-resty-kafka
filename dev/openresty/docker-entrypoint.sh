#!/bin/bash
###########

sh -c "/usr/local/openresty/bin/nginxReloader.sh &"
exec "$@"