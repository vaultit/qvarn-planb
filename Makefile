PYTHON_3.6=python3.6

.PHONY: env
env: env/.done requirements.txt

env/bin/pip:
	$(PYTHON_3.6) -m venv env
	env/bin/pip install --upgrade pip wheel setuptools

env/.done: env/bin/pip setup.py requirements-dev.txt
	env/bin/pip install -r requirements-dev.txt -e .
	touch env/.done

env/bin/pip-compile: env/bin/pip
	env/bin/pip install pip-tools

requirements-dev.txt: env/bin/pip-compile requirements.in requirements-dev.in
	env/bin/pip-compile --no-index requirements.in requirements-dev.in -o requirements-dev.txt

requirements.txt: env/bin/pip-compile requirements.in
	env/bin/pip-compile --no-index requirements.in -o requirements.txt

.PHONY: run
run: env
	env/bin/qvarn run --host 0.0.0.0

.PHONY: test
test: env
	env/bin/py.test tests --cov=qvarn --cov-report=term-missing

.PHONY: dist
dist: env/bin/pip
	env/bin/python setup.py sdist bdist_wheel

.PHONY: postgres
postgres:
	docker run \
	  --rm \
	  --detach \
	  --name pg96 \
	  --publish 5432:5432 \
	  -e POSTGRES_USER=qvarn \
	  -e POSTGRES_PASSWORD=qvarn \
	  -e POSTGRES_DB=planbtest \
	  postgres:9.6-alpine
#	while ! docker exec -it pg96 nc -z localhost 5432; do echo "Waiting for database..." & sleep 1; done
#	docker exec \
	  -it pg96 psql \
	  -U postgres \
	  -c "CREATE DATABASE planb;" \
	  -c "GRANT ALL PRIVILEGES ON DATABASE planb TO qvarn;"
	