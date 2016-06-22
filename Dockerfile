FROM debian:jessie

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
RUN echo "deb http://repo.mongodb.org/apt/debian wheezy/mongodb-org/3.0 main" > /etc/apt/sources.list.d/mongodb-org-3.0.list
RUN apt-get update && \
	apt-get install -y curl gcc git python3-dev mongodb-org-server redis-server && \
	apt-get clean -y && \
	rm -rf /var/lib/apt/lists/*

RUN curl -s "https://bootstrap.pypa.io/get-pip.py" | python3

ADD requirements-heroku.txt /app/requirements-heroku.txt
ADD requirements-base.txt /app/requirements-base.txt
ADD requirements-dev.txt /app/requirements-dev.txt
ADD requirements-dashboard.txt /app/requirements-dashboard.txt

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
