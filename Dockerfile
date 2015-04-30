FROM ubuntu:14.04

RUN echo "apt sources updated on 2015-04-29"

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
RUN echo "deb http://repo.mongodb.org/apt/ubuntu "$(lsb_release -sc)"/mongodb-org/3.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.0.list
# RUN echo "deb http://repo.mongodb.org/apt/debian wheezy/mongodb-org/3.0 main" | tee /etc/apt/sources.list.d/mongodb-org-3.0.list

# Do this in one command to avoid creating a large image
RUN apt-get update && \
apt-get upgrade -y && \
apt-get install -y sudo redis-server mongodb-org-server gcc make g++ build-essential libc6-dev tcl curl adduser python python-dev strace git software-properties-common nginx graphviz && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
rm -rf /var/cache/* && \
rm -rf /usr/share/doc/* && \
rm -rf /usr/share/man/*

# python pip seems to bug on ubuntu 14.04 with IncompleteRead
RUN curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py" && python get-pip.py

# RUN curl http://download.redis.io/releases/redis-2.6.16.tar.gz > redis.tar.gz && \
# mkdir /redis && tar -zxf redis.tar.gz -C /redis --strip 1 && rm redis.tar.gz && \
# cd /redis && make && make install && ln -s /usr/local/bin/redis-server /usr/bin/redis-server

# FS is read-only!
# RUN sysctl vm.overcommit_memory=1

RUN mkdir -p /data/db
VOLUME ["/data"]

ADD requirements-heroku.txt requirements-heroku.txt
ADD requirements-base.txt requirements-base.txt
ADD requirements-dev.txt requirements-dev.txt
ADD requirements-dashboard.txt requirements-dashboard.txt
RUN pip install -r requirements-heroku.txt && \
pip install -r requirements-base.txt && \
pip install -r requirements-dev.txt && \
pip install -r requirements-dashboard.txt && \
rm -rf /root/.cache

# Redis
EXPOSE 6379
# MongoDB
EXPOSE 27017
# Dashboard
EXPOSE 5555
# Worker monitoring port
EXPOSE 20020
# docs
EXPOSE 8000
