FROM debian:jessie

#
# httpredir.debian.org is often unreliable
# https://github.com/docker-library/buildpack-deps/issues/40
#

RUN echo \
   'deb ftp://ftp.us.debian.org/debian/ jessie main\n \
    deb ftp://ftp.us.debian.org/debian/ jessie-updates main\n \
    deb http://security.debian.org jessie/updates main\n' \
    > /etc/apt/sources.list

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
RUN echo "deb http://repo.mongodb.org/apt/debian wheezy/mongodb-org/3.0 main" > /etc/apt/sources.list.d/mongodb-org-3.0.list
RUN apt-get update && \
	apt-get install -y --no-install-recommends \
				curl \
				gcc \
				python-dev \
				python-pip \
				python3-pip \
    			python3-dev \
    			git \
    			vim \
				mongodb-org-server \
				nginx redis-server \
	&& \
	apt-get clean -y && \
	rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade --ignore-installed pip
RUN pip3 install --upgrade --ignore-installed pip

ADD requirements-heroku.txt /app/requirements-heroku.txt
ADD requirements-base.txt /app/requirements-base.txt
ADD requirements-dev.txt /app/requirements-dev.txt
ADD requirements-dashboard.txt /app/requirements-dashboard.txt

RUN pip3 install -r /app/requirements-heroku.txt && \
	pip3 install -r /app/requirements-base.txt && \
	pip3 install -r /app/requirements-dev.txt && \
	pip3 install -r /app/requirements-dashboard.txt && \
	rm -rf ~/.cache

RUN pip install -r /app/requirements-heroku.txt && \
	pip install -r /app/requirements-base.txt && \
	pip install -r /app/requirements-dev.txt && \
	pip install -r /app/requirements-dashboard.txt && \
	rm -rf ~/.cache

RUN mkdir -p /data/db

VOLUME ["/data"]
WORKDIR /app

# Redis and MongoDB services
EXPOSE 6379 27017

# Dashboard, monitoring and docs
EXPOSE 5555 20020 8000
