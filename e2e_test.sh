#! /bin/bash
# Convenience script for running e2e tests
cd tests
docker-compose up -d
sleep 30
. setup_e2e_tests.sh
cd ..
tox -e e2e_gs_tests,clean
cd tests
docker-compose down
cd ..