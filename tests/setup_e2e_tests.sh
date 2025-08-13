#!/usr/bin/env bash

docker-compose exec postgis /bin/bash -c "psql -U postgres -c \"CREATE DATABASE tds_tests WITH OWNER postgres;\""
docker-compose exec postgis /bin/bash -c "psql -U postgres -d tds_tests -c  \"CREATE EXTENSION postgis;\""
curl -u admin:geoserver -H 'Accept: application/xml' -H 'Content-Type: application/xml' -X PUT -d '<global><proxyBaseUrl>http://127.0.0.1:8181/geoserver</proxyBaseUrl></global>' http://127.0.0.1:8181/geoserver/rest/settings.xml