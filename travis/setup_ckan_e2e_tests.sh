# Ckan Test Services
echo "CLONING CKAN..."
git clone https://github.com/ckan/ckan.git ckan
pushd ckan
echo "CHECKOUT VERSION 2.8.0"
git checkout tags/ckan-2.8.0
pushd contrib/docker
echo "CONFIGURING DOCKER COMPOSE ENVIRONMENT"
cp .env.template .env
echo "BUILD CKAN CONTAINERS AND RUN"
docker-compose up -d --build
sleep 30
docker restart ckan
sleep 10
echo "RUNNING DOCKERS:"
docker ps -a
echo "CKAN DOCKER LOGS:"
docker logs ckan
echo "CREATING ADMIN USER..."
docker exec -it ckan /bin/bash -c "echo \"y\" | /usr/local/bin/ckan-paster --plugin=ckan sysadmin -c /etc/ckan/production.ini add ${CKAN_USERNAME} email=\"foo@tethysplatform.org\" password=\"${CKAN_PASSWORD} apikey="11111111-1111-1111-1111-111111111111"\"\""
echo "GETTING API KEY..."
export CKAN_APIKEY=$(sudo docker exec -it db /bin/bash -c "psql -U postgres -c \"SELECT apikey FROM ckan.public.user WHERE name = '${CKAN_USERNAME}'\" ckan" | sed -n 3p | awk '{$1=$1;print}')
echo "$CKAN_APIKEY"
popd
popd