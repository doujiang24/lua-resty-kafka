OPENRESTY_PREFIX=/usr/local/openresty-debug

PREFIX ?=          /usr/local
LUA_INCLUDE_DIR ?= $(PREFIX)/include
LUA_LIB_DIR ?=     $(PREFIX)/lib/lua/$(LUA_VERSION)
INSTALL ?= install

.PHONY: all test install

all: ;

install: all
	$(INSTALL) -d $(DESTDIR)/$(LUA_LIB_DIR)/resty/kafka
	$(INSTALL) lib/resty/kafka/*.lua $(DESTDIR)/$(LUA_LIB_DIR)/resty/kafka

test: all
	PATH=$(OPENRESTY_PREFIX)/nginx/sbin:$$PATH prove -I ./../test-nginx/lib -r t/

