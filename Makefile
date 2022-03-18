.PHONY : docs doctests lint sdist sync tests

build : lint tests docs doctests sdist

lint :
	flake8

tests :
	pytest -v --cov=beaver_build --cov-fail-under=100 --cov-report=term-missing --cov-report=html -m "not timing"
# Run timing tests without coverage.
	pytest -v -m timing

docs :
	sphinx-build . docs/_build -n

doctests :
	sphinx-build -b doctest . docs/_build

sync : requirements.txt
	pip-sync

requirements.txt : requirements.in setup.py test_requirements.txt
	pip-compile -v -o $@ $<

test_requirements.txt : test_requirements.in setup.py
	pip-compile -v -o $@ $<

VERSION :
	python generate_version.py

sdist : VERSION
	python setup.py sdist
	twine check dist/*.tar.gz
