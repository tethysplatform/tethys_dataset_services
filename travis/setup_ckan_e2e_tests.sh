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
docker exec -it ckan /bin/bash -c "echo \"y\" | /usr/local/bin/ckan-paster --plugin=ckan sysadmin -c /etc/ckan/production.ini add ${CKAN_USERNAME} email=\"foo@tethysplatform.org\" password=\"${CKAN_PASSWORD}\"\"" &> /tmp/sysadmin.json
export CKAN_APIKEY=$(sudo docker exec -it db /bin/bash -c "psql -U postgres -c \"SELECT apikey FROM ckan.public.user WHERE name = '${CKAN_USERNAME}'\" ckan" | sed -n 3p)
echo $CKAN_APIKEY
popd
popd