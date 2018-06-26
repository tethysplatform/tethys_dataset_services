"""
********************************************************************************
* Name: geoserver_engine_e2e_tests.py
* Author: nswain
* Created On: June 25, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *  # noqa: F403, F401

import random
import string
import unittest
from sqlalchemy.engine import create_engine
from geoserver.catalog import Catalog as GeoServerCatalog

# from tethys_dataset_services.engines import GeoServerSpatialDatasetEngine
from tethys_dataset_services.tests.test_config import TEST_GEOSERVER_DATASET_SERVICE
from tethys_dataset_services.tests.test_config import TEST_POSTGIS_SERVICE


def random_string_generator(size):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


class GeoServerDatasetEngineEnd2EndTests(unittest.TestCase):

    def setUp(self):
        # GeoServer
        self.gs_endpoint = TEST_GEOSERVER_DATASET_SERVICE['ENDPOINT']
        self.gs_username = TEST_GEOSERVER_DATASET_SERVICE['USERNAME']
        self.gs_password = TEST_GEOSERVER_DATASET_SERVICE['PASSWORD']
        self.catalog = GeoServerCatalog(self.gs_endpoint, username=self.gs_username, password=self.gs_password)

        # Setup a testing workspace
        self.workspace_name = random_string_generator(10)
        self.workspace_uri = 'http://www.tethysplatform.org/tds-test-workspace'
        self.catalog.create_workspace(self.workspace_name, self.workspace_uri)

        # Setup Postgis database connection
        self.engine = create_engine(TEST_POSTGIS_SERVICE['URL'])
        self.connection = self.engine.connect()
        self.transaction = self.connection.begin()

    def tearDown(self):
        # Clean up GeoServer
        workspace = self.catalog.get_workspace(self.workspace_name)
        self.catalog.delete(workspace, recurse=True, purge=True)

        # Clean up Postgis database
        self.transaction.rollback()
        self.connection.close()
        self.engine.dispose()

    def setup_postgis_table(self):
        """
        Creates table in the database named "points" with two points added to it. The table has three columns:
        "id", "name", and "geometry." Use this table for the tests that require a database.
        """
        self.postgis_table_name = 'points'
        geom_table_sql = "CREATE TABLE {table} (" \
                         "id integer CONSTRAINT points_primary_key PRIMARY KEY, " \
                         "name varchar(20)" \
                         "); " \
                         "SELECT AddGeometryColumn('public', '{table}', 'geometry', 4326, 'POINT', 2);". \
            format(
                table=self.postgis_table_name
            )

        self.connection.execute(geom_table_sql)

        insert_sql = "INSERT INTO {table} VALUES ({id}, '{name}', ST_GeomFromText('POINT({lon} {lat})', 4326));"
        rows = [
            {"id": 1, "name": "Aquaveo", "lat": 40.276039, "lon": -111.651120},
            {"id": 2, "name": "BYU", "lat": 40.252335, "lon": -111.649326},
        ]

        for r in rows:
            sql = insert_sql.format(
                table=self.postgis_table_name,
                id=r['id'],
                name=r['name'],
                lat=r['lat'],
                lon=r['lon']
            )
            self.connection.execute(sql)

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
        # Use existing layers and styles in geoserver:
        # layers: sf:roads, sf:bugsites, sf:streams;
        # styles: simple_roads, capitals, simple_streams
        # call methods: create_layer_group, list_layer_groups, get_layer_group, delete_layer_group
        raise NotImplementedError()

    def test_create_workspace(self):
        # DO NOT MOCK
        # workspace_id='this_is_a_test' uri='http://www.tethysplatform.org/test'
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

        # DB table setup
        self.setup_postgis_table()
        raise NotImplementedError()

    def test_create_postgis_feature_resource(self):
        # DO NOT MOCK
        # Use testing_config.TEST_POSTGIS_SERVICE for db credentials
        # call methods: create_postgis_feature_resource, list_stores, get_store, delete_store
        self.setup_postgis_table()
        raise NotImplementedError()

    def test_create_sql_view(self):
        # DO NOT MOCK
        # Use testing_config.TEST_POSTGIS_SERVICE for db credentials
        # call methods: create_sql_view, list_resources, list_stores, list_layers
        self.setup_postgis_table()
        raise NotImplementedError()
