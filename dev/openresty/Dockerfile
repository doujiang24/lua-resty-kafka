
FROM ubuntu:18.04


ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get install inotify-tools -y

RUN    echo "deb-src http://archive.ubuntu.com/ubuntu bionic main" >> /etc/apt/sources.list
RUN    sed 's/main$/main universe/' -i /etc/apt/sources.list
RUN    apt-get update
RUN    apt-get upgrade -y
RUN    apt-get -y install wget vim git libpq-dev tcpdump luarocks
RUN    luarocks install inspect

# Openresty (Nginx)
RUN  apt-get -y build-dep nginx \
  && apt-get -q -y clean && rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*
RUN  wget https://openresty.org/download/openresty-1.19.3.2.tar.gz \
  && tar zxvf openresty-1.19.3.2.tar.gz \
  && wget https://github.com/Kong/kong-build-tools/archive/master.tar.gz \
  && tar zvfx master.tar.gz \
  && cd openresty-1.19.3.2/bundle \ 
  && for i in ../../kong-build-tools-master/openresty-patches/patches/1.19.3.2/*.patch; do patch -p1 < $i; done \
  && cd ../ \
  && ./configure --with-luajit  --with-http_addition_module --with-http_dav_module --with-http_geoip_module --with-http_gzip_static_module --with-http_image_filter_module --with-http_realip_module --with-http_stub_status_module --with-http_ssl_module --with-http_sub_module --with-http_xslt_module --with-ipv6 --with-http_postgres_module --with-pcre-jit \
  && make \
  && make install 

RUN ln -s /usr/local/openresty/nginx/sbin/nginx /usr/sbin/nginx
COPY ./reload_nginx.sh /usr/local/openresty/bin/nginxReloader.sh
COPY ./docker-entrypoint.sh /usr/local/openresty/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/openresty/bin/nginxReloader.sh
RUN chmod +x /usr/local/openresty/bin/docker-entrypoint.sh


ENTRYPOINT [ "/usr/local/openresty/bin/docker-entrypoint.sh" ]
CMD ["/usr/local/openresty/bin/openresty", "-g", "daemon off;"]