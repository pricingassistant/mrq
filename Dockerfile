FROM pricingassistant/ubuntu:13.10

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
RUN echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | tee /etc/apt/sources.list.d/10gen.list

RUN apt-get update && echo "Updated on 2014-03-15"
RUN apt-get upgrade -y

RUN apt-get install -y gcc make g++ build-essential libc6-dev tcl curl adduser mongodb-10gen python python-pip python-dev strace git software-properties-common libev-dev nginx graphviz

# Then add PPAs (after software-properties-common is installed)
# RUN add-apt-repository -y ppa:pypy/ppa
# RUN apt-get update
# RUN apt-get install -y pypy pypy-dev

RUN curl http://download.redis.io/releases/redis-2.6.16.tar.gz > redis.tar.gz
RUN mkdir /redis && tar -zxf redis.tar.gz -C /redis --strip 1 && rm redis.tar.gz
RUN cd /redis && make && make test && make install && ln -s /usr/local/bin/redis-server /usr/bin/redis-server

# FS is read-only!
# RUN sysctl vm.overcommit_memory=1

RUN mkdir -p /data/db
VOLUME ["/data"]

ADD requirements-heroku.txt requirements-heroku.txt
RUN pip install --use-mirrors -r requirements-heroku.txt

ADD requirements-base.txt requirements-base.txt
RUN pip install --use-mirrors -r requirements-base.txt

ADD requirements-dev.txt requirements-dev.txt
RUN pip install --use-mirrors -r requirements-dev.txt

ADD requirements-dashboard.txt requirements-dashboard.txt
RUN pip install --use-mirrors -r requirements-dashboard.txt

# TODO remove me. https://github.com/kevin1024/pytest-httpbin/issues/9
RUN pip install pytest-httpbin==0.0.3

EXPOSE 6379
EXPOSE 27017
EXPOSE 5555
EXPOSE 20020
