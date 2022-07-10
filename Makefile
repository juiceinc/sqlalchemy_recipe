.PHONY: all docs tests precommit .FORCE

all: docs flake8 tests

docs:
	cd docs && make html

tests:
	py.test --cov-config .coveragerc --cov=recipe tests/

release:
	# 1) Make sure tests pass
	# 3) bumpversion
	# 4) release
	rm -f dist/*
	python setup.py bdist_wheel sdist
	#twine upload -r pypi dist/*

docker_build
	docker build -t sqlalchemy-recipe .

docker_run
	docker run -p 8080:80 sqlalchemy-recipe