install:
	pip install .
	python setup.py sdist bdist_wheel

test:
	black --check .
	pytest --cov 'orm' --cov-fail-under 95

format:
	black .

doc:
	pip install .[docs]
	sphinx-apidoc -f -o docsrc/source orm
	make -C docsrc github

clean:
	rm -rf build
	rm -rf dist
	rm -rf orm.egg-info
	find . -name *.pyc -delete
	find . -name __pycache__ -delete

coverage:
	coverage erase
	pytest --cov 'orm'
	coverage html

version:
	bump2version --config-file .bumpversion.cfg $(BUMP)

all: clean install format coverage

