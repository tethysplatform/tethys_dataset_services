=======================
Tethys Dataset Services
=======================

Tethys datasets provides Python programming interface for dataset services such as CKAN and HydroShare.

Installation
------------

Tethys Datasets Services can be installed via pip or downloading the source. To install via pip::

  pip install tethys_dataset_services

To install via download::

  git clone https://github.com/CI-WATER/django-tethys_dataset_services.git
  cd tethys_dataset_services
  python setup.py install

Tests
-----

To run the tests

Usage
-----

::

  from tethys_dataset_services.engines import CkanDatasetEngine

  engine = CkanDatasetEngine(endpoint='',
                             apikey='')


