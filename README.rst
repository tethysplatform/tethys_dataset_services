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

To run the tests you will need to edit the ``test_config.py`` file located at ``tethys_dataset_services.tests.test_config.py`` with an appropriate CKAN endpoint and API key.

Usage
-----

::

  from tethys_dataset_services.engines import CkanDatasetEngine

  engine = CkanDatasetEngine(endpoint='http://<ckan_host>/api/3/action',
                             apikey='G3taN@p|k3Y')

  result = engine.list_datasets()


