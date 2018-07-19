# GeoServer Test Services
docker pull ciwater/geoserver:2.8.2-clustered
docker pull mdillon/postgis:9.6
docker run -d --name=test_postgis -p $POSTGIS_PORT:5432 -e POSTGRES_PASSWORD=$POSTGIS_PASS -e POSTGRES_USER=$POSTGIS_USER mdillon/postgis:9.6
docker run -d --name=test_geoserver -p 8181:8181 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 8084:8084 -e ENABLED_NODES=1 -e REST_NODES=1 -e MAX_MEMORY=512 -e MIN_MEMORY=512 -e NUM_CORES=2 -e MAX_TIMEOUT=60 ciwater/geoserver:2.8.2-clustered
sleep 30
docker exec test_postgis /bin/bash -c "psql -U postgres -c \"CREATE DATABASE ${POSTGIS_DB} WITH OWNER ${POSTGIS_USER};\""
docker exec test_postgis /bin/bash -c "psql -U postgres -d $POSTGIS_DB -c  \"CREATE EXTENSION postgis;\""
"curl -u $GEOSERVER_USERNAME:$GEOSERVER_PASSWORD -H 'Accept: application/xml' -H 'Content-Type: application/xml' -X PUT -d '<global><proxyBaseUrl>http://127.0.0.1:8181/geoserver</proxyBaseUrl></global>' http://127.0.0.1:8181/geoserver/rest/settings.xml"
