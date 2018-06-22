#!/usr/bin/env bash
echo "Running Tests for GeoServer Engine..."
coverage run --rcfile=coverage.ini setup.py test -s tethys_dataset_services.tests.geoserver_engine_tests.TestGeoServerDatasetEngine
echo "Running Tests for CKAN Engine..."
#coverage run --rcfile=coverage.ini setup.py test -s tethys_dataset_services.tests.ckan_engine_tests.TestCkanDatasetEngine
coverage report -m
flake8