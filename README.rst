===============
Tethys Datasets
===============

Tethys datasets provides an api for Dataset Services such as CKAN and HydroShare so this data can be incorporated into
your website easily. It also provides a simple data browser for viewing the data in the Dataset Services you have linked
to. Though, part of Tethys Platform, this Django app can be installed independently.

Installation
------------

Tethys Datasets can be installed via pip or downloading the source. To install via pip::

  pip install django-tethys_datasets

To install via download::

  git clone https://github.com/CI-WATER/django-tethys_datasets.git
  cd django-tethys_datasets
  python setup.py install

Django Configuration
--------------------

1. Add "tethys_datasets" to your INSTALLED_APPS setting like so::

  INSTALLED_APPS = (
      ...
      'tethys_datasets',
  )

2. Include the URLconf in your project urls.py::

  url(r'^datasets/', include('tethys_datasets.urls')),

3. Add the TETHYS_DATASET_SERVICES parameter to your settings.py with appropriate configuration values for the dataset
services you wish to plug into::

  TETHYS_DATASET_SERVICES = {
      'ckan_example': {
          'ENGINE': 'tethys_datasets.engines.CkanDatasetEngine',
          'ENDPOINT': 'http://www.example.com/api/3/action',
          'APIKEY': 'a-R3llY-n1Ce-@Pi-keY',
      },
      'hydroshare_example': {
          'ENGINE': 'tethys_datasets.engines.HydroShareDatasetEngine',
          'ENDPOINT': 'www.hydroshare.org/api',
          'USERNAME': 'username',
          'PASSWORD': 'password'
      }
  }
