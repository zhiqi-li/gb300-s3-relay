.PHONY: test lint build

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

lint:
	python3 -m ruff check .

build:
	python3 -m build
