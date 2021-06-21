#!/bin/bash
###########

sh -c "nginxReloader.sh &"
exec "$@"