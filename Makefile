OPENRESTY_PREFIX=/usr/local/openresty-debug

PREFIX ?=          /usr/local
LUA_INCLUDE_DIR ?= $(PREFIX)/include
LUA_LIB_DIR ?=     $(PREFIX)/lib/lua/$(LUA_VERSION)
INSTALL ?= install

.PHONY: all install

all: ;

install: all
	$(INSTALL) -d $(DESTDIR)/$(LUA_LIB_DIR)/resty/kafka
	$(INSTALL) lib/resty/kafka/*.lua $(DESTDIR)/$(LUA_LIB_DIR)/resty/kafka

luarocks:
	luarocks make

setup-certs:
	cd dev/; bash kafka-generate-ssl-automatic.sh; cd -

devup: setup-certs
	docker-compose -f dev/docker-compose.yaml  -f dev/docker-compose.dev.yaml up -d

test: devup
	docker-compose -f dev/docker-compose.yaml  -f dev/docker-compose.dev.yaml exec openresty luarocks make
	docker-compose -f dev/docker-compose.yaml  -f dev/docker-compose.dev.yaml exec openresty busted

devdown:
	docker-compose -f dev/docker-compose.yaml  -f dev/docker-compose.dev.yaml down --remove-orphans

devshell:
	docker-compose -f dev/docker-compose.yaml  -f dev/docker-compose.dev.yaml exec openresty /bin/bash