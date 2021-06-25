#!/bin/bash
set -e

(
	# TODO: add checks for existence etc
	cd /srv/lib
	luarocks make
)

exec "$@"