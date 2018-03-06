docker:
	docker build -t pricingassistant/mrq-env .
	docker build -t pricingassistant/mrq -f Dockerfile-with-code .

docker_push:
	docker push pricingassistant/mrq-env:latest
	docker push pricingassistant/mrq:latest

test: docker
	sh -c "docker run --rm -i -t -p 27017:27017 -p 6379:6379 -p 5555:5555 -p 20020:20020 -v `pwd`:/app:rw -w /app pricingassistant/mrq-env python -m pytest tests/ -v --instafail"

test3: docker
	sh -c "docker run --rm -i -t -p 27017:27017 -p 6379:6379 -p 5555:5555 -p 20020:20020 -v `pwd`:/app:rw -w /app pricingassistant/mrq-env python3 -m pytest tests/ -v --instafail"

testpypy: docker
	sh -c "docker run --rm -i -t -p 27017:27017 -p 6379:6379 -p 5555:5555 -p 20020:20020 -v `pwd`:/app:rw -w /app pricingassistant/mrq-env /pypy/bin/pypy -m pytest tests/ -v --instafail"

shell:
	sh -c "docker run --rm -i -t -p 27017:27017 -p 6379:6379 -p 5555:5555 -p 20020:20020 -p 8000:8000 -v `pwd`:/app:rw -w /app pricingassistant/mrq-env bash"

reshell:
	# Reconnect in the current taskqueue container
	sh -c 'docker exec -t -i `docker ps | grep pricingassistant/mrq-env | cut -f 1 -d " "` bash'

shell_noport:
	sh -c "docker run --rm -i -t -v `pwd`:/app:rw -w /app pricingassistant/mrq-env bash"

docs_serve:
	sh -c "docker run --rm -i -t -p 8000:8000 -v `pwd`:/app:rw -w /app pricingassistant/mrq-env mkdocs serve"

lint: docker
	docker run -i -t -v `pwd`:/app:rw -w /app pricingassistant/mrq-env pylint -j 0 --init-hook="import sys; sys.path.append('.')" --rcfile .pylintrc mrq

linterrors: docker
	docker run -i -t -v `pwd`:/app:rw -w /app pricingassistant/mrq-env pylint -j 0 --errors-only --init-hook="import sys; sys.path.append('.')" --rcfile .pylintrc mrq

linterrors3: docker
	docker run -i -t -v `pwd`:/app:rw -w /app pricingassistant/mrq-env python3 -m pylint -j 0 --errors-only --init-hook="import sys; sys.path.append('.')" --rcfile .pylintrc mrq

virtualenv:
	virtualenv venv --distribute

deps:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pip install -r requirements-dashboard.txt

clean:
	find . -path ./venv -prune -o -name "*.pyc" -exec rm {} \;
	find . -name __pycache__ | xargs rm -r

build_dashboard:
	cd mrq/dashboard/static && npm install && mkdir -p bin && npm run build

dashboard:
	python mrq/dashboard/app.py

stack:
	mongod --smallfiles --noprealloc --nojournal &
	redis-server &
	python mrq/dashboard/app.py &

pep8:
	autopep8 --max-line-length 99 -aaaaaaaa --diff --recursive mrq
	echo "Now run 'make autopep8' to apply."

autopep8:
	autopep8 --max-line-length 99 -aaaaaaaa --in-place --recursive mrq

pypi: linterrors
	python setup.py sdist upload

build_docs:
	python scripts/propagate_docs.py

ensureindexes:
	mrq-run mrq.basetasks.indexes.EnsureIndexes
