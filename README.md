[![Build Status](https://travis-ci.org/tethysplatform/tethys_dataset_services.svg)](https://travis-ci.org/tethysplatform/tethys_dataset_services)
[![Build status](https://ci.appveyor.com/api/projects/status/ehh3lx289lfj4ue5?svg=true)](https://ci.appveyor.com/project/TethysPlatform/tethys-dataset-services)
[![Coverage Status](https://coveralls.io/repos/github/tethysplatform/tethys_dataset_services/badge.svg)](https://coveralls.io/github/tethysplatform/tethys_dataset_services)

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
pip install -e .[tests]
tox
```

## Usage

```
from tethys_dataset_services.engines import CkanDatasetEngine

engine = CkanDatasetEngine(endpoint='http://<ckan_host>/api/3/action',
                         apikey='G3taN@p|k3Y')

result = engine.list_datasets()
```


