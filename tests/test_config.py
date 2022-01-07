"""
IMPORTANT: DO NOT EDIT THIS FILE. IF YOU NEED TO CHANGE THE VALUE OF A VARIABLE HERE, SET THE APPROPRIATE
ENVIRONMENT VARIABLE.
"""
import os
from sqlalchemy.engine.url import make_url

# IMPORTANT: DO NOT EDIT THIS FILE. IF YOU NEED TO CHANGE THE VALUE OF A VARIABLE HERE, SET THE APPROPRIATE
# ENVIRONMENT VARIABLE.

TEST_CKAN_DATASET_SERVICE = {
    'ENGINE': 'tethys_dataset_services.engines.CkanDatasetEngine',
    'ENDPOINT': os.environ.get('CKAN_ENDPOINT', 'http://127.0.0.1:5000/api/3/action/'),
    'APIKEY': os.environ.get('CKAN_APIKEY', 'my-api-key'),
    'USERNAME': os.environ.get('CKAN_USERNAME', 'tethys'),
    'PASSWORD': os.environ.get('CKAN_PASSWORD', 'password'),
}

# IMPORTANT: DO NOT EDIT THIS FILE. IF YOU NEED TO CHANGE THE VALUE OF A VARIABLE HERE, SET THE APPROPRIATE
# ENVIRONMENT VARIABLE.

TEST_HYDRO_SHARE_DATASET_SERVICE = {
    'ENGINE': '',
    'ENDPOINT': os.environ.get('HYDROSHARE_ENDPOINT', 'https://www.hydroshare.org/'),
    'APIKEY': os.environ.get('HYDROSHARE_APIKEY', 'my-api-key'),
    'USERNAME': os.environ.get('HYDROSHARE_USERNAME', 'tethys'),
    'PASSWORD': os.environ.get('HYDROSHARE_PASSWORD', 'pass'),
}

# IMPORTANT: DO NOT EDIT THIS FILE. IF YOU NEED TO CHANGE THE VALUE OF A VARIABLE HERE, SET THE APPROPRIATE
# ENVIRONMENT VARIABLE.

TEST_GEOSERVER_DATASET_SERVICE = {
    'ENGINE': 'tethys_dataset_services.engines.GeoServerSpatialDatasetEngine',
    'ENDPOINT': os.environ.get('GEOSERVER_ENDPOINT', 'http://localhost:8181/geoserver/rest/'),
    'USERNAME': os.environ.get('GEOSERVER_USERNAME', 'admin'),
    'PASSWORD': os.environ.get('GEOSERVER_PASSWORD', 'geoserver')
}

# IMPORTANT: DO NOT EDIT THIS FILE. IF YOU NEED TO CHANGE THE VALUE OF A VARIABLE HERE, SET THE APPROPRIATE
# ENVIRONMENT VARIABLE.

# NOTE: THIS DATABASE NEEDS TO BE SETUP MANUALLY WITH POSTGIS EXTENSION ENABLED
_URL = os.environ.get('POSTGIS_URL', 'postgresql://postgres:mysecretpassword@172.17.0.1:5432/tds_tests')
_url_object = make_url(_URL)
_PUBLIC_URL = os.environ.get('POSTGIS_PUBLIC_URL', 'postgresql://postgres:mysecretpassword@localhost:5432/tds_tests')
_public_url_object = make_url(_PUBLIC_URL)

TEST_POSTGIS_SERVICE = {
    'URL': _URL,
    'DRIVER': _url_object.drivername,
    'USERNAME': _url_object.username,
    'PASSWORD': _url_object.password,
    'HOST': _url_object.host,
    'PORT': _url_object.port,
    'DATABASE': _url_object.database,
    'PUBLIC_URL': _PUBLIC_URL,
    'PUBLIC_HOST': _public_url_object.host,
    'PUBLIC_PORT': _public_url_object.port,
}

# IMPORTANT: DO NOT EDIT THIS FILE. IF YOU NEED TO CHANGE THE VALUE OF A VARIABLE HERE, SET THE APPROPRIATE
# ENVIRONMENT VARIABLE.