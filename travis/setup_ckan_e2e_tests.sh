# Ckan Test Services
git clone https://github.com/ckan/ckan.git ckan
pushd ckan
git checkout tags/ckan-2.8.0
pushd contrib/docker
cp .env.template .env
docker-compose up -d --build
sleep 30
docker restart ckan
sleep 10
docker ps -a
docker logs ckan
docker exec -it ckan /bin/bash -c "echo \"y\" | /usr/local/bin/ckan-paster --plugin=ckan sysadmin -c /etc/ckan/production.ini add ${CKAN_USERNAME} email=\"foo@tethysplatform.org\" password=\"${CKAN_PASSWORD}\" apikey=\"${CKAN_APIKEY}\"" &> /tmp/sysadmin.json
cat /tmp/sysadmin.json
popd
popd