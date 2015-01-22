# Needed for testing

TEST_CKAN_DATASET_SERVICE = {
    'ENGINE': 'tethys_dataset_services.engines.CkanDatasetEngine',
    'ENDPOINT': 'http://ciwckan.chpc.utah.edu/api/3/action',
    'APIKEY': '003654e6-cd89-46a6-9035-28e4037b44d6',
    'USERNAME': '',
    'PASSWORD': ''
}

TEST_HYDRO_SHARE_DATASET_SERVICE = {
    'ENGINE': '',
    'ENDPOINT': '',
    'APIKEY': '',
    'USERNAME': '',
    'PASSWORD': ''
}

TEST_GEOSERVER_DATASET_SERVICE = {
    'ENGINE': 'tethys_dataset_services.engines.GeoServerSpatialDatasetEngine',
    'ENDPOINT': 'http://192.168.59.103:8181/geoserver/rest',
    'USERNAME': 'admin',
    'PASSWORD': 'geoserver'
}