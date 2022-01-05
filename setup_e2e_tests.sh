#!/usr/bin/env bash
docker rm -f tds_postgis
docker rm -f tds_geoserver
docker run -d --name=tds_postgis -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_USER=postgres postgis/postgis:9.6-2.5
docker run -d --name=tds_geoserver -p 8181:8181 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 8084:8084 -e ENABLED_NODES=1 -e REST_NODES=1 -e MAX_MEMORY=512 -e MIN_MEMORY=512 -e NUM_CORES=2 -e MAX_TIMEOUT=60 tethysplatform/geoserver
sleep 60
docker exec tds_postgis /bin/bash -c "psql -U postgres -c \"CREATE DATABASE tds_tests WITH OWNER postgres;\""
docker exec tds_postgis /bin/bash -c "psql -U postgres -d tds_tests -c  \"CREATE EXTENSION postgis;\""
curl -u admin:geoserver -H 'Accept: application/xml' -H 'Content-Type: application/xml' -X PUT -d '<global><proxyBaseUrl>http://127.0.0.1:8181/geoserver</proxyBaseUrl></global>' http://127.0.0.1:8181/geoserver/rest/settings.xml