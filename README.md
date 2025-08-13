[![Unit Tests](https://github.com/tethysplatform/tethys_dataset_services/actions/workflows/unit_tests.yml/badge.svg?branch=tethys4_update)](https://github.com/tethysplatform/tethys_dataset_services/actions/workflows/unit_tests.yml)
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

## Versioning and Releases

This project uses `setuptools_scm` for automatic versioning based on git tags and has migrated from `setup.cfg` to `pyproject.toml` for modern Python packaging standards. The version is automatically determined from the latest git tag and the number of commits since that tag.

### Creating a Release

To create a new release:

1. Create and push a git tag with the version number (following semantic versioning):
   ```bash
   git tag 2.4.0
   git push origin 2.4.0
   ```

2. The GitHub Action workflow will automatically:
   - Run all tests on Python 3.10, 3.11, 3.12, and 3.13
   - Build the package
   - Publish to PyPI

### Development Versions

During development, the version will include the commit hash and distance from the last tag (e.g., `2.4.1.dev5+g1234567`).

### PyPI Configuration

For automatic publishing to work, you need to configure PyPI API token in your repository secrets as `PYPI_API_TOKEN`, or set up trusted publishing in your PyPI account settings.

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
tox -e e2e_gs_tests
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


