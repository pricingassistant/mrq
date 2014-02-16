FROM pricingassistant/ubuntu:13.10

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
RUN echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | tee /etc/apt/sources.list.d/10gen.list

RUN apt-get update && echo "Updated on 2014-01-15"
RUN apt-get upgrade -y

RUN apt-get install -y gcc make g++ build-essential libc6-dev tcl curl adduser mongodb-10gen python python-pip python-dev

RUN curl http://download.redis.io/releases/redis-2.6.16.tar.gz > redis.tar.gz
RUN mkdir /redis && tar -zxf redis.tar.gz -C /redis --strip 1 && rm redis.tar.gz

RUN cd /redis && make && make test && make install && ln -s /usr/local/bin/redis-server /usr/bin/redis-server

RUN echo 1 > /proc/sys/vm/overcommit_memory

RUN mkdir -p /data/db
VOLUME ["/data"]

RUN apt-get install -y strace git

ADD requirements.txt requirements.txt
RUN pip install --use-mirrors -r requirements.txt

ADD requirements-dev.txt requirements-dev.txt
RUN pip install --use-mirrors -r requirements-dev.txt

EXPOSE 6379
EXPOSE 27017
