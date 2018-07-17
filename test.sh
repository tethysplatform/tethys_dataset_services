#!/usr/bin/env bash
rm .coverage
echo "Running Unit Tests..."
coverage run -a --rcfile=coverage.ini setup.py test -s tethys_dataset_services.tests.unit_tests
coverage report -m
flake8