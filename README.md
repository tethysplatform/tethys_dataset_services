[![Unit Tests](https://github.com/tethysplatform/tethys_dataset_services/actions/workflows/unit_tests.yml/badge.svg?branch=tethys4_update)]()
[![Coverage](https://coveralls.io/repos/github/tethysplatform/tethys_dataset_services/badge.svg)](https://coveralls.io/github/tethysplatform/tethys_dataset_services)

# Tethys Dataset Services

Tethys datasets provides Python programming interface for dataset services such as GeoServer, CKAN and HydroShare.

---
**NOTE**

Tethys Dataset Services versions 2.0.0 and up will only support Python 3. For Python 2 support see version 1.7.0.

---

## Installation

Tethys Datasets Services can be installed via conda or downloading the source. To install via conda:

```
conda install tethys_dataset_services
```

To install via source:

```
git clone https://github.com/CI-WATER/django-tethys_dataset_services.git
cd tethys_dataset_services
pip install .
```

To install a development (editable) version:

```
git clone https://github.com/CI-WATER/django-tethys_dataset_services.git
cd tethys_dataset_services
pip install --editable .
```

## Tests

Tests are executed using tox:

```
pip install .[tests]
tox
```

## End-to-End Tests

End-to-end tests are not run automatically, b/c they require some additional set up. They can be run as follows.

1. Install Docker: https://docs.docker.com/get-docker/

2. Install Docker Compose: https://docs.docker.com/compose/install/


3. Run Docker Compose to create test containers:

```
cd tests
docker-compose up -d
```

4. Wait at least 30 seconds for the docker containers to settle down, then run the setup script to create the database tables:

```
. setup_e2e_tests.sh
```

5. From the directory with the tox.ini, run the tests using tox:

```
cd ..
tox -e e2e_geoserver_tests
```

6. It is recommended that after each run, you refresh the Docker containers. Run the following command to remove them:

```
cd tests
docker-compose down
```

Then repeat steps 3-5.

## Usage

```
from tethys_dataset_services.engines import CkanDatasetEngine

engine = CkanDatasetEngine(endpoint='http://<ckan_host>/api/3/action',
                         apikey='G3taN@p|k3Y')

result = engine.list_datasets()
```


