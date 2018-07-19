# Ckan Test Services
git clone https://github.com/ckan/ckan.git ckan
cd ckan
git checkout tags/ckan-2.8.0
cd contrib/docker
docker-compose up -d --build
sleep 30
docker ps -a
