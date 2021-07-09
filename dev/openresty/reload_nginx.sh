#!/bin/bash
###########

while true
do
 inotifywait --exclude .swp --recursive -e create -e modify -e delete -e move /usr/local/openresty/nginx/conf/conf.d /usr/local/lib/lua/lua-resty-kafka/lib
 nginx -t
 if [ $? -eq 0 ]
 then
  echo "Detected Nginx Configuration Change"
  echo "Executing: nginx -s reload"
  nginx -s reload
 fi
done