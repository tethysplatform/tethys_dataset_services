[![Build Status](https://travis-ci.org/tethysplatform/tethys_dataset_services.svg?branch=python3)](https://travis-ci.org/tethysplatform/tethys_dataset_services)
[![Build status](https://ci.appveyor.com/api/projects/status/ehh3lx289lfj4ue5?svg=true)](https://ci.appveyor.com/project/TethysPlatform/tethys-dataset-services)

# Tethys Dataset Services

Tethys datasets provides Python programming interface for dataset services such as CKAN and HydroShare.

## Installation

Tethys Datasets Services can be installed via pip or downloading the source. To install via pip::

```
pip install tethys_dataset_services
```

To install via download::

```
git clone https://github.com/CI-WATER/django-tethys_dataset_services.git
cd tethys_dataset_services
pip install -r requirements-py<version>.txt
python setup.py install
```

## Tests

To run tests execute:

```
. test.sh
```

## Usage

```
from tethys_dataset_services.engines import CkanDatasetEngine

engine = CkanDatasetEngine(endpoint='http://<ckan_host>/api/3/action',
                         apikey='G3taN@p|k3Y')

result = engine.list_datasets()
```


