#!/usr/bin/env bash
export PYTHONTRACEMALLOC=1
rm .coverage
coverage run -a --rcfile=coverage.ini setup.py test -s tethys_dataset_services.tests.e2e_tests.geoserver_engine_e2e_tests.GeoServerDatasetEngineEnd2EndTests
coverage report -m