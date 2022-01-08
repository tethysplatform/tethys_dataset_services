#! /bin/bash
# Convenience script for running e2e tests
docker-compose up -d
sleep 30
. setup_e2e_tests.sh
tox -e e2e_gs_tests,clean
docker-compose down