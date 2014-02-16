init:
	docker build -t mrq/mrq_local .

test: init
	sh -c "docker run -rm -i -t -p 27017:27017 -p 6379:6379 -v `pwd`:/app:rw -w /app mrq/mrq_local py.test tests/ -s -v"

shell: init
	sh -c "docker run -rm -i -t -p 27017:27017 -p 6379:6379 -v `pwd`:/app:rw -w /app mrq/mrq_local bash"

lint:
	pylint --init-hook="import sys; sys.path.append('.')" --rcfile .pylintrc mrq

linterrors:
	pylint --errors-only --init-hook="import sys; sys.path.append('.')" -d E1103 --rcfile .pylintrc mrq

virtualenv:
	virtualenv venv --distribute

deps:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

clean:
	find . -path ./venv -prune -o -name "*.pyc" -exec rm {} \;
