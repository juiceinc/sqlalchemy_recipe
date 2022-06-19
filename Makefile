.PHONY: all docs tests precommit .FORCE

all: docs flake8 tests

docs:
	cd docs && make html

autodocs:
	sphinx-autobuild docs docs/_build/html/ --port 8001

tests:
	py.test --cov-config .coveragerc --cov=recipe tests/

release:
	# 1) Make sure tests pass
	# 3) bumpversion
	# 4) release
	rm -f dist/*
	python setup.py bdist_wheel sdist
	#twine upload -r pypi dist/*
