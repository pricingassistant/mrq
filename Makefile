init:
	docker build -t mrq/mrq_local .

test: init
	sh -c "docker run -rm -i -t -p 27017:27017 -p 6379:6379 -p 5555:5555 -p 20000:20000 -v `pwd`:/app:rw -w /app mrq/mrq_local py.test tests/ -s -v"

shell: init
	sh -c "docker run -rm -i -t -p 27017:27017 -p 6379:6379 -p 5555:5555 -p 20000:20000 -v `pwd`:/app:rw -w /app mrq/mrq_local bash"

lint:
	pylint --init-hook="import sys; sys.path.append('.')" --rcfile .pylintrc mrq

linterrors:
	pylint --errors-only --init-hook="import sys; sys.path.append('.')" -d E1103 --rcfile .pylintrc mrq

virtualenv:
	virtualenv venv --distribute

virtualenv_pypy:
	virtualenv -p /usr/bin/pypy pypy --distribute

deps:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pip install -r requirements-dashboard.txt

deps_pypy:
	pip install git+git://github.com/schmir/gevent@pypy-hacks
	pip install cffi
	pip install git+git://github.com/gevent-on-pypy/pypycore
	export GEVENT_LOOP=pypycore.loop
	pip install -r requirements-pypy.txt

clean:
	find . -path ./venv -prune -o -name "*.pyc" -exec rm {} \;
	find . -name __pycache__ | xargs rm -r

dashboard:
	gunicorn -w 4 -b 0.0.0.0:5555 -k gevent mrq.dashboard.app:app

dashboard_dev:
	python mrq/dashboard/app.py

stack:
	mongod &
	redis-server &
	python mrq/dashboard/app.py &
