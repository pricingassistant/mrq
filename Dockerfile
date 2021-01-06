FROM debian:buster-slim

#
# httpredir.debian.org is often unreliable
# https://github.com/docker-library/buildpack-deps/issues/40
#

# RUN echo \
#    'deb ftp://ftp.us.debian.org/debian/ jessie main\n \
#     deb ftp://ftp.us.debian.org/debian/ jessie-updates main\n \
#     deb http://security.debian.org jessie/updates main\n' \
#     > /etc/apt/sources.list

RUN apt-get update && \
	apt-get install -y --no-install-recommends \
				curl \
				gcc \
				python-dev \
				python-pip \
				python-setuptools \
				python3-pip \
    			python3-dev \
    			python3-setuptools \
    			make \
    			git \
    			vim \
    			bzip2 \
				nginx redis-server \
				g++ \
	&& \
	apt-get clean -y && \
	rm -rf /var/lib/apt/lists/*

RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt-get install -y --no-install-recommends nodejs

RUN curl -sL https://www.mongodb.org/static/pgp/server-4.2.asc | apt-key add -
RUN echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.2 main" > /etc/apt/sources.list.d/mongodb-org-4.2.list
RUN apt-get update && apt-get install -y --no-install-recommends mongodb-org

# Download pypy
RUN curl -sL 'https://github.com/squeaky-pl/portable-pypy/releases/download/pypy-7.2.0/pypy-7.2.0-linux_x86_64-portable.tar.bz2' > /pypy.tar.bz2 && tar jxvf /pypy.tar.bz2 && rm -rf /pypy.tar.bz2 && mv /pypy* /pypy

# Upgrade pip
RUN pip install --upgrade --ignore-installed pip
RUN pip3 install --upgrade --ignore-installed pip
RUN /pypy/bin/pypy -m ensurepip

ADD requirements-heroku.txt /app/requirements-heroku.txt
ADD requirements-base.txt /app/requirements-base.txt
ADD requirements-dev.txt /app/requirements-dev.txt
ADD requirements-dashboard.txt /app/requirements-dashboard.txt

RUN python3 -m pip install -r /app/requirements-heroku.txt && \
	python3 -m pip install -r /app/requirements-base.txt && \
	python3 -m pip install -r /app/requirements-dev.txt && \
	python3 -m pip install -r /app/requirements-dashboard.txt && \
	rm -rf ~/.cache

RUN python -m pip install -r /app/requirements-heroku.txt && \
	python -m pip install -r /app/requirements-base.txt && \
	python -m pip install -r /app/requirements-dev.txt && \
	python -m pip install -r /app/requirements-dashboard.txt && \
	rm -rf ~/.cache

RUN /pypy/bin/pip install -r /app/requirements-heroku.txt && \
	/pypy/bin/pip install -r /app/requirements-base.txt && \
	/pypy/bin/pip install -r /app/requirements-dev.txt && \
	/pypy/bin/pip install -r /app/requirements-dashboard.txt && \
	rm -rf ~/.cache

RUN mkdir -p /data/db

RUN ln -s /app/mrq/bin/mrq_run.py /usr/bin/mrq-run
RUN ln -s /app/mrq/bin/mrq_worker.py /usr/bin/mrq-worker
RUN ln -s /app/mrq/bin/mrq_agent.py /usr/bin/mrq-agent
RUN ln -s /app/mrq/dashboard/app.py /usr/bin/mrq-dashboard

ENV PYTHONPATH /app

VOLUME ["/data"]
WORKDIR /app

# Redis and MongoDB services
EXPOSE 6379 27017

# Dashboard, monitoring and docs
EXPOSE 5555 20020 8000
