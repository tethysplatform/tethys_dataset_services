"""
********************************************************************************
* Name: geoserver_engine_e2e_tests.py
* Author: nswain
* Created On: June 25, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""
import os
import random
import string
import unittest
import geoserver
from tethys_dataset_services.engines import GeoServerSpatialDatasetEngine
from tethys_dataset_services.tests.test_config import TEST_GEOSERVER_DATASET_SERVICE
from tethys_dataset_services.tests.test_config import TEST_POSTGIS_SERVICE


def random_string_generator(size):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


class GeoServerDatasetEngineEnd2EndTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_create_shapefile_resource_base(self):
        # DO NOT MOCK
        # test
        # call methods: create_shapefile_resource, list_resources, get_resource, delete_resource
        raise NotImplementedError()

    def test_create_shapefile_resource_zip(self):
        # DO NOT MOCK
        # test1.zip
        # call methods: create_shapefile_resource, list_layers, get_layer, delete_layer
        raise NotImplementedError()

    def test_create_shapefile_resource_upload(self):
        # DO NOT MOCK
        # Use in memory file list: test.shp and friends
        # call methods: create_shapefile_resource, list_stores, get_store, delete_store
        raise NotImplementedError()

    def test_create_coverage_resource_arcgrid(self):
        # DO NOT MOCK
        # precip30min.zip
        # call methods: create_coverage_resource, list_resources, get_resource, delete_resource
        raise NotImplementedError()

    def test_create_coverage_resource_grassgrid(self):
        # DO NOT MOCK
        # my_grass.zip
        # call methods: create_coverage_resource, list_layers, get_layer, delete_layer
        raise NotImplementedError()

    def test_create_coverage_resource_geotiff(self):
        # DO NOT MOCK
        # adem.tif
        # call methods: create_coverage_resource, list_stores, get_store, delete_store
        raise NotImplementedError()

    def test_create_coverage_resource_world_file_tif(self):
        # DO NOT MOCK
        # pk50095.zip
        # call methods: create_coverage_resource, list_layers, get_layer, delete_layer
        raise NotImplementedError()

    def test_create_coverage_resource_world_file_jpeg(self):
        # DO NOT MOCK
        # usa.zip
        # call methods: create_coverage_resource, list_stores, get_store, delete_store
        raise NotImplementedError()

    def test_create_coverage_resource_upload(self):
        # DO NOT MOCK
        # Use in memory file list: precip30min.prj & precip30min.asc
        # call methods: create_coverage_resource, list_resources, get_resource, delete_resource
        raise NotImplementedError()

    def test_create_layer_group(self):
        # DO NOT MOCK
        # Use existing layers and styles in geoserver: layers: sf:roads, sf:bugsites, sf:streams; styles: simple_roads, capitals, simple_streams
        # call methods: create_layer_group, list_layer_groups, get_layer_group, delete_layer_group
        raise NotImplementedError()

    def test_create_workspace(self):
        # DO NOT MOCK
        # workspace_id='tds_test_workspace' uri='http://www.tethysplatform.org/test'
        # call methods: create_workspace, list_workspaces, get_workspace, delete_workspace
        raise NotImplementedError()

    def test_create_style(self):
        # DO NOT MOCK
        # style_id='test_points'; Use files/point.sld for sld.
        # call methods: create_style, list_styles, get_style, delete_style
        raise NotImplementedError()

    def test_link_sqlalchemy_db_to_geoserver(self):
        # DO NOT MOCK
        # Use testing_config.TEST_POSTGIS_SERVICE for db credentials
        # call methods: link_sqlalchemy_db_to_geoserver, list_stores, get_store, delete_store
        raise NotImplementedError()

    def create_postgis_feature_resource(self):
        # DO NOT MOCK
        # Use testing_config.TEST_POSTGIS_SERVICE for db credentials
        # call methods: create_postgis_feature_resource, list_stores, get_store, delete_store
        raise NotImplementedError()
