#!/usr/bin/env bash
rm .coverage
echo "Running Tests for GeoServer Engine..."
coverage run -a --rcfile=coverage.ini setup.py test -s tethys_dataset_services.tests.geoserver_engine_tests.TestGeoServerDatasetEngine
echo "Running Tests for CKAN Engine..."
coverage run -a --rcfile=coverage.ini setup.py test -s tethys_dataset_services.tests.ckan_engine_tests.TestCkanDatasetEngine
echo "Running Tests for Utilities..."
coverage run -a --rcfile=coverage.ini setup.py test -s tethys_dataset_services.tests.utilities_test.TestUtilities
coverage report -m
flake8