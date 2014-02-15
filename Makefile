init:
	docker build -t mrq/mrq_local .

test: init
	sh -c "docker run -rm -i -t -p 27017:27017 -p 6379:6379 -v `pwd`:/app:rw -w /app mrq/mrq_local bash"

lint:
	pylint --init-hook="import sys; sys.path.append('.')" --rcfile .pylintrc mrq

linterrors:
	pylint --errors-only --init-hook="import sys; sys.path.append('.')" -d E1103 --rcfile .pylintrc mrq
