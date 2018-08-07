from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *  # noqa: F403, F401

import os
import sys
import random
import string
import unittest
import mock
import geoserver
import requests
from sqlalchemy import create_engine
from tethys_dataset_services.engines import GeoServerSpatialDatasetEngine

if sys.version_info[0] == 3:
    from io import StringIO
else:
    from StringIO import StringIO

try:
    from tethys_dataset_services.tests.test_config import TEST_GEOSERVER_DATASET_SERVICE

except ImportError:
    print('ERROR: To perform tests, you must create a file in the "tests" package called "test_config.py". In this file'
          'provide a dictionary called "TEST_GEOSERVER_DATASET_SERVICE" with keys "ENDPOINT", "USERNAME", and '
          '"PASSWORD".')
    exit(1)


def random_string_generator(size):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


def mock_get_style(name, workspace=None):
    mock_style = mock.NonCallableMagicMock(workspace=workspace)
    mock_style.name = name
    return mock_style


def mock_get_resource(name, **kwargs):
    if 'workspace' or 'store' in kwargs:
        mock_resource = mock.NonCallableMagicMock()
        mock_resource.name = name
        if 'workspace' in kwargs:
            mock_resource.workspace = kwargs['workspace']
        if 'store' in kwargs:
            mock_resource.store = kwargs['store']
        return mock_resource
    else:
        raise AssertionError('Did not get expected keyword arguments: {}'.format(list(kwargs)))


def mock_get_resource_create_postgis_feature_resource(name, **kwargs):
    if 'workspace' in kwargs:
        raise geoserver.catalog.FailedRequestError()
    elif 'store' in kwargs:
        mock_resource = mock.NonCallableMagicMock()
        mock_resource.name = name
        mock_resource.store = kwargs['store']
        return mock_resource
    else:
        raise AssertionError('Did not get expected keyword arguments: {}'.format(list(kwargs)))


class MockResponse(object):
    def __init__(self, status_code, text=None, json=None, reason=None):
        self.status_code = status_code
        self.text = text
        self.json_obj = json
        self.reason = reason

    def json(self):
        return self.json_obj


class TestGeoServerDatasetEngine(unittest.TestCase):

    def setUp(self):
        # Globals
        self.debug = False
        self.counter = 0

        # Files
        self.tests_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.files_root = os.path.join(self.tests_root, 'files')

        self.shapefile_name = 'test'
        self.shapefile_base = os.path.join(self.files_root, 'shapefile', self.shapefile_name)

        # Create Test Engine
        self.endpoint = TEST_GEOSERVER_DATASET_SERVICE['ENDPOINT']
        self.engine = GeoServerSpatialDatasetEngine(endpoint=self.endpoint,
                                                    username=TEST_GEOSERVER_DATASET_SERVICE['USERNAME'],
                                                    password=TEST_GEOSERVER_DATASET_SERVICE['PASSWORD'])

        # Catalog
        self.catalog_endpoint = 'http://localhost:8181/geoserver/'
        self.mock_catalog = mock.NonCallableMagicMock(gs_base_url=self.catalog_endpoint)

        # Workspaces
        self.workspace_name = 'a-workspace'

        # Store
        self.store_name = 'a-store'
        self.mock_store = mock.NonCallableMagicMock()  #: Needs to pass not callable test
        # the "name" attribute needs to be set after create b/c name is a constructor argument
        # http://blog.tunarob.com/2017/04/27/mock-name-attribute/
        self.mock_store.name = self.store_name

        # Default Style
        self.default_style_name = 'a-style'
        self.mock_default_style = mock.NonCallableMagicMock(workspace=self.workspace_name)
        self.mock_default_style.name = self.default_style_name

        # Styles
        self.style_names = ['points', 'lines']
        self.mock_styles = []
        for sn in self.style_names:
            mock_style = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_style.name = sn
            self.mock_styles.append(mock_style)

        # Resources
        self.resource_names = ['foo', 'bar', 'goo']
        self.mock_resources = []
        for rn in self.resource_names:
            mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_resource.name = rn
            mock_resource.store = self.mock_store
            self.mock_resources.append(mock_resource)

        # Layers
        self.layer_names = ['baz', 'bat', 'jazz']
        self.mock_layers = []
        for ln in self.layer_names:
            mock_layer = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_layer.name = ln
            mock_layer.store = self.mock_store
            mock_layer.default_style = self.mock_default_style
            mock_layer.styles = self.mock_styles
            self.mock_layers.append(mock_layer)

        # Layer groups
        self.layer_group_names = ['boo', 'moo']
        self.mock_layer_groups = []
        for lgn in self.layer_group_names:
            mock_layer_group = mock.NonCallableMagicMock(
                workspace=self.workspace_name,
                catalog=self.mock_catalog,
                dom='fake-dom',
                layers=self.layer_names,
                style=self.style_names
            )
            mock_layer_group.name = lgn
            self.mock_layer_groups.append(mock_layer_group)

        # Workspaces
        self.workspace_names = ['b-workspace', 'c-workspace']
        self.mock_workspaces = []
        for wp in self.workspace_names:
            mock_workspace = mock.NonCallableMagicMock()
            mock_workspace.name = wp
            self.mock_workspaces.append(mock_workspace)

        # Stores
        self.store_names = ['b-store', 'c-store']
        self.mock_stores = []
        for sn in self.store_names:
            mock_store_name = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_store_name.name = sn
            self.mock_stores.append(mock_store_name)

    def mock_upload_fail_three_times(self, *args, **kwargs):
        self.counter += 1

        if self.counter < 3:
            raise geoserver.catalog.UploadError()

    def assert_valid_response_object(self, response_object):
        # Response object should be a dictionary with the keys 'success' and either 'result' if success is True
        # or 'error' if success is False
        self.assertIsInstance(response_object, dict)
        self.assertIn('success', response_object)

        if isinstance(response_object, dict) and 'success' in response_object:
            if response_object['success'] is True:
                self.assertIn('result', response_object)
            elif response_object['success'] is False:
                self.assertIn('error', response_object)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_resources(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.return_value = self.mock_resources

        # Execute
        response = self.engine.list_resources(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        for n in self.resource_names:
            self.assertIn(n, result)

        mc.get_resources.called_with(store=None, workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_resources_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.return_value = self.mock_resources

        # Execute
        response = self.engine.list_resources(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Returns list
        self.assertIsInstance(result, list)

        # List of dictionaries
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn('name', r)
            self.assertIn(r['name'], self.resource_names)
            self.assertIn('workspace', r)
            self.assertEqual(self.workspace_name, r['workspace'])
            self.assertIn('store', r)
            self.assertEqual(self.store_name, r['store'])

        mc.get_resources.called_with(store=None, workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_resources_ambiguous_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.side_effect = geoserver.catalog.AmbiguousRequestError()

        # Execute
        response = self.engine.list_resources(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        mc.get_resources.called_with(store=None, workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_resources_multiple_stores_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.side_effect = TypeError()

        # Execute
        response = self.engine.list_resources(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])
        self.assertIn('Multiple stores found named', response['error'])

        mc.get_resources.called_with(store=None, workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_layers(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layers.return_value = self.mock_layers

        # Execute
        response = self.engine.list_layers(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        for n in self.layer_names:
            self.assertIn(n, result)

        mc.get_layers.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_layers_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layers.return_value = self.mock_layers

        # Execute
        response = self.engine.list_layers(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Returns list
        self.assertIsInstance(result, list)

        # List of dictionaries
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn('name', r)
            self.assertIn(r['name'], self.layer_names)
            self.assertIn('workspace', r)
            self.assertEqual(self.workspace_name, r['workspace'])
            self.assertIn('store', r)
            self.assertEqual(self.store_name, r['store'])
            self.assertIn('default_style', r)
            w_default_style = '{}:{}'.format(self.workspace_name, self.default_style_name)
            self.assertEqual(w_default_style, r['default_style'])
            self.assertIn('styles', r)
            w_styles = ['{}:{}'.format(self.workspace_name, style) for style in self.style_names]
            for s in r['styles']:
                self.assertIn(s, w_styles)

        mc.get_layers.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_layer_groups(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.return_value = self.mock_layer_groups

        # Execute
        response = self.engine.list_layer_groups(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer group listed
        for r in result:
            self.assertIn(r, self.layer_group_names)

        mc.get_layergroups.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_layer_groups_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.return_value = self.mock_layer_groups

        # Execute
        response = self.engine.list_layer_groups(with_properties=True, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Returns list
        self.assertIsInstance(result, list)

        # List of dictionaries
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn('name', r)
            self.assertIn(r['name'], self.layer_group_names)
            self.assertIn('workspace', r)
            self.assertEqual(self.workspace_name, r['workspace'])
            self.assertIn('catalog', r)
            self.assertEqual(self.catalog_endpoint, r['catalog'])
            self.assertIn('layers', r)
            self.assertEqual(self.layer_names, r['layers'])
            self.assertNotIn('dom', r)

        mc.get_layergroups.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_workspaces(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspaces.return_value = self.mock_workspaces

        # Execute
        response = self.engine.list_workspaces(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer group listed
        for r in result:
            self.assertIn(r, self.workspace_names)

        mc.get_workspaces.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_stores(self, mock_catalog):
        mc = mock_catalog()
        mc.get_stores.return_value = self.mock_stores

        # Execute
        response = self.engine.list_stores(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer group listed
        for r in result:
            self.assertIn(r, self.store_names)

        mc.get_stores.assert_called_with(workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_stores_invalid_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_stores.return_value = self.mock_stores
        mc.get_stores.side_effect = AttributeError()

        workspace = 'invalid'

        # Execute
        response = self.engine.list_stores(workspace=workspace, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # False
        self.assertFalse(response['success'])
        self.assertIn('Invalid workspace', response['error'])
        mc.get_stores.assert_called_with(workspace=workspace)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_styles(self, mock_catalog):
        mc = mock_catalog()
        mc.get_styles.return_value = self.mock_styles

        # Execute
        response = self.engine.list_styles(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        for n in self.style_names:
            self.assertIn(n, result)

        mc.get_styles.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_styles_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_styles.return_value = self.mock_styles

        # Execute
        response = self.engine.list_styles(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn('name', r)
            self.assertIn(r['name'], self.style_names)
            self.assertIn('workspace', r)
            self.assertEqual(self.workspace_name, r['workspace'])
        mc.get_styles.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource(self, mock_catalog):
        mc = mock_catalog()
        mc.get_default_workspace().name = self.workspace_name
        mc.get_resource.return_value = self.mock_resources[0]

        # Execute
        response = self.engine.get_resource(resource_id=self.resource_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn('name', r)
        self.assertIn(r['name'], self.resource_names)
        self.assertIn('workspace', r)
        self.assertEqual(self.workspace_name, r['workspace'])
        self.assertIn('store', r)
        self.assertEqual(self.store_name, r['store'])

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None,
                                           workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource_with_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        response = self.engine.get_resource(resource_id=resource_id,
                                            debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn('name', r)
        self.assertIn(r['name'], self.resource_names)
        self.assertIn('workspace', r)
        self.assertEqual(self.workspace_name, r['workspace'])
        self.assertIn('store', r)
        self.assertEqual(self.store_name, r['store'])

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_resource(resource_id=self.resource_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # False
        self.assertFalse(response['success'])

        # Expect Error
        r = response['error']

        # Properties
        self.assertIn('not found', r)

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None,
                                           workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.side_effect = geoserver.catalog.FailedRequestError('Failed Request')
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_resource(resource_id=self.resource_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # False
        self.assertFalse(response['success'])

        # Expect Error
        r = response['error']

        # Properties
        self.assertIn('Failed Request', r)

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None,
                                           workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource_with_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Execute
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        response = self.engine.get_resource(resource_id=resource_id,
                                            store_id=self.store_name,
                                            debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn('name', r)
        self.assertIn(r['name'], self.resource_names)
        self.assertIn('workspace', r)
        self.assertEqual(self.workspace_name, r['workspace'])
        self.assertIn('store', r)
        self.assertEqual(self.store_name, r['store'])

        mc.get_resource.assert_called_with(name=self.resource_names[0],
                                           store=self.store_name,
                                           workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.get')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer(self, mock_catalog, mock_get):
        mc = mock_catalog()
        mc.get_layer.return_value = self.mock_layers[0]

        mock_get.return_value = MockResponse(200, text='<GeoServerLayer><foo>bar</foo></GeoServerLayer>')

        # Execute
        response = self.engine.get_layer(layer_id=self.layer_names[0], store_id=self.store_name,
                                         debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn('name', r)
        self.assertEqual(self.layer_names[0], r['name'])
        self.assertIn('store', r)
        self.assertEqual(self.store_name, r['store'])
        self.assertIn('default_style', r)
        self.assertIn(self.default_style_name, r['default_style'])
        self.assertIn('styles', r)
        w_styles = ['{}:{}'.format(self.workspace_name, style) for style in self.style_names]
        for s in r['styles']:
            self.assertIn(s, w_styles)

        self.assertIn('tile_caching', r)
        self.assertEqual({'foo': 'bar'}, r['tile_caching'])

        mc.get_layer.assert_called_with(name=self.layer_names[0])
        mock_get.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_layer(layer_id=self.layer_names[0], store_id=self.store_name, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('not found', r)

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Execute
        response = self.engine.get_layer(layer_id=self.layer_names[0],
                                         store_id=self.store_name,
                                         debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertEqual(r, 'Failed Request')

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer_group(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroup.return_value = self.mock_layer_groups[0]

        # Execute
        response = self.engine.get_layer_group(layer_group_id=self.layer_group_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # List of dictionaries
        self.assertIn('workspace', r)
        self.assertEqual(self.workspace_name, r['workspace'])
        self.assertIn('catalog', r)
        self.assertEqual(self.catalog_endpoint, r['catalog'])
        self.assertIn('layers', r)
        self.assertEqual(self.layer_names, r['layers'])
        self.assertNotIn('dom', r)

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer_group_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroup.return_value = None

        # Execute
        response = self.engine.get_layer_group(layer_group_id=self.layer_group_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('not found', r)

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer_group_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroup.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Execute
        response = self.engine.get_layer_group(layer_group_id=self.layer_group_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertEqual(r, 'Failed Request')

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn('name', r)
        self.assertIn(r['name'], self.store_names)
        self.assertIn('workspace', r)
        self.assertEqual(self.workspace_name, r['workspace'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_store_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError('Failed Request')
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('Failed Request', r)

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_store_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('not found', r)

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_style(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.return_value = self.mock_styles[0]
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn('name', r)
        self.assertIn(r['name'], self.style_names)
        self.assertIn('workspace', r)
        self.assertEqual(self.workspace_name, r['workspace'])

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_style_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('not found', r)

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_style_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.side_effect = geoserver.catalog.FailedRequestError('Failed Request')
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('Failed Request', r)

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.return_value = self.mock_workspaces[0]

        # Execute
        response = self.engine.get_workspace(workspace_id=self.workspace_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn('name', r)
        self.assertIn(r['name'], self.workspace_names[0])

        mc.get_workspace.assert_called_with(name=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_workspace_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.return_value = None

        # Execute
        response = self.engine.get_workspace(workspace_id=self.workspace_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('not found', r)

        mc.get_workspace.assert_called_with(name=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_workspace_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Execute
        response = self.engine.get_workspace(workspace_id=self.workspace_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('Failed Request', r)

        mc.get_workspace.assert_called_with(name=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_resource(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            title='foo',
            geometry='points'
        )

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = 'lines'

        # Execute
        response = self.engine.update_resource(resource_id=resource_id,
                                               title=new_title,
                                               geometry=new_geometry,
                                               debug=self.debug)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(result['geometry'], new_geometry)

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_resource_no_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            title='foo',
            geometry='points'
        )
        mc.get_default_workspace().name = self.workspace_name

        # Setup
        resource_id = self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = 'lines'

        # Execute
        response = self.engine.update_resource(resource_id=resource_id,
                                               title=new_title,
                                               geometry=new_geometry,
                                               debug=self.debug)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(result['geometry'], new_geometry)

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_resource_style(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            styles=['style_name'],
        )
        mc.get_style.side_effect = mock_get_style

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_styles = ['new_style_name']

        # Execute
        response = self.engine.update_resource(resource_id=resource_id,
                                               styles=new_styles,
                                               debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['styles'], new_styles)

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_resource_style_colon(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            styles=['1:2'],
        )
        mc.get_style.side_effect = mock_get_style

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_styles = ['11:22']

        # Execute
        response = self.engine.update_resource(resource_id=resource_id,
                                               styles=new_styles,
                                               debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['styles'], new_styles)

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_resource_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = 'lines'

        # Execute
        response = self.engine.update_resource(resource_id=resource_id,
                                               title=new_title,
                                               geometry=new_geometry,
                                               debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Fail
        self.assertFalse(response['success'])

        # Expect Error
        r = response['error']

        # Properties
        self.assertIn('Failed Request', r)

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_resource_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            store=self.store_name,
            title='foo',
            geometry='points'
        )

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = 'lines'

        # Execute
        response = self.engine.update_resource(resource_id=resource_id,
                                               store=self.store_name,
                                               title=new_title,
                                               geometry=new_geometry,
                                               debug=self.debug)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(result['geometry'], new_geometry)
        self.assertEqual(result['store'], self.store_name)

        mc.get_resource.assert_called_with(name=self.resource_names[0],
                                           store=self.store_name,
                                           workspace=self.workspace_name)
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_layer(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0],
            title='foo',
            geometry='points'
        )

        # Setup
        new_title = random_string_generator(15)
        new_geometry = 'lines'

        # Execute
        response = self.engine.update_layer(layer_id=self.layer_names[0],
                                            title=new_title,
                                            geometry=new_geometry,
                                            debug=self.debug)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(result['geometry'], new_geometry)

        mc.get_layer.assert_called_with(name=self.layer_names[0])
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_layer_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.side_effect = geoserver.catalog.FailedRequestError('Failed Request')
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0],
            title='foo',
            geometry='points'
        )

        # Setup
        new_title = random_string_generator(15)
        new_geometry = 'lines'

        # Execute
        response = self.engine.update_layer(layer_id=self.layer_names[0],
                                            title=new_title,
                                            geometry=new_geometry,
                                            debug=self.debug)
        # Validate response object
        self.assert_valid_response_object(response)

        # Fail
        self.assertFalse(response['success'])

        # Expect Error
        r = response['error']

        # Properties
        self.assertIn('Failed Request', r)

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_layer_with_tile_caching_params(self, mock_catalog, mock_post):
        mc = mock_catalog()
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0],
            title='foo',
            geometry='points'
        )
        mock_post.return_value = MockResponse(200)

        # Setup
        new_title = random_string_generator(15)
        new_geometry = 'lines'
        tile_caching = {'foo': 'bar'}

        # Execute
        response = self.engine.update_layer(layer_id=self.layer_names[0],
                                            title=new_title,
                                            geometry=new_geometry,
                                            debug=self.debug,
                                            tile_caching=tile_caching)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(result['geometry'], new_geometry)
        self.assertIn('foo', result['tile_caching'])
        self.assertEqual(result['tile_caching']['foo'], 'bar')

        mc.get_layer.assert_called_with(name=self.layer_names[0])
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_layer_with_tile_caching_params_not_200(self, mock_catalog, mock_post):
        mc = mock_catalog()
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0],
            title='foo',
            geometry='points'
        )
        mock_post.return_value = MockResponse(500, text='server error')

        # Setup
        new_title = random_string_generator(15)
        new_geometry = 'lines'
        tile_caching = {'foo': 'bar'}

        # Execute
        response = self.engine.update_layer(layer_id=self.layer_names[0],
                                            title=new_title,
                                            geometry=new_geometry,
                                            debug=self.debug,
                                            tile_caching=tile_caching)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        self.assertIn('server error', response['error'])

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_layer_group(self, mock_catalog):
        mc = mock_catalog()
        mock_layer_group = mock.NonCallableMagicMock(
            layers=self.layer_names
        )
        mock_layer_group.name = self.layer_group_names[0]
        mc.get_layergroup.return_value = mock_layer_group

        # Setup
        new_layers = random_string_generator(15)

        # Execute
        response = self.engine.update_layer_group(layer_group_id=self.layer_group_names[0],
                                                  layers=new_layers,
                                                  debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Properties
        self.assertEqual(result['layers'], new_layers)

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0], workspace=None)
        mc.save.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_update_layer_group_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroup.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Setup
        new_layers = random_string_generator(15)

        # Execute
        response = self.engine.update_layer_group(layer_group_id=self.mock_layer_groups[0],
                                                  layers=new_layers,
                                                  debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Fail
        self.assertFalse(response['success'])

        # Expect Error
        r = response['error']

        # Properties
        self.assertIn('Failed Request', r)

        mc.get_layergroup.assert_called_with(name=self.mock_layer_groups[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_with_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        resource_id = '{}:{}'.format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id, store_id=self.mock_store)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=self.mock_store,
                                           workspace=self.workspace_name)
        mc.delete.assert_called_with(config_object=self.mock_resources[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_without_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]
        mc.get_default_workspace().name = self.workspace_name
        resource_id = self.resource_names[0]

        # Execute
        response = self.engine.delete_resource(resource_id, store_id=self.mock_store)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=self.mock_store,
                                           workspace=self.workspace_name)
        mc.delete.assert_called_with(config_object=self.mock_resources[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]
        mc.delete.side_effect = geoserver.catalog.FailedRequestError()

        resource_id = '{}:{}'.format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id, store_id=self.mock_store)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])
        mc.delete.assert_called_with(config_object=self.mock_resources[0], purge=False, recurse=False)
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=self.mock_store,
                                           workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_does_not_exist(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = None

        resource_id = '{}:{}'.format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id, store_id=self.store_name)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])
        self.assertIn('GeoServer object does not exist', response['error'])
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=self.store_name,
                                           workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_layer(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = self.mock_layers[0]

        # Execute
        response = self.engine.delete_layer(self.layer_names[0], store_id=self.store_name)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])
        mc.get_layer.assert_called_with(name=self.layer_names[0])
        mc.delete.assert_called_with(config_object=self.mock_layers[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_layer_group(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroup.return_value = self.mock_layer_groups[0]

        # Do delete
        response = self.engine.delete_layer_group(layer_group_id=self.layer_group_names[0])

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response['success'])
        self.assertIsNone(response['result'])
        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0], workspace=None)
        mc.delete.assert_called_with(config_object=self.mock_layer_groups[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.return_value = self.mock_workspaces[0]

        # Do delete
        response = self.engine.delete_workspace(workspace_id=self.workspace_names[0])

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response['success'])
        self.assertIsNone(response['result'])

        mc.get_workspace.assert_called_with(self.workspace_names[0])
        mc.delete.assert_called_with(config_object=self.mock_workspaces[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]
        mc.get_default_workspace().name = self.workspace_name

        # Do delete
        response = self.engine.delete_store(store_id=self.store_names[0])

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response['success'])
        self.assertIsNone(response['result'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_name)
        mc.delete.assert_called_with(config_object=self.mock_stores[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_store_failed_request(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        mc.get_default_workspace().name = self.workspace_name

        # Do delete
        response = self.engine.delete_store(store_id=self.store_names[0])

        # Failure Check
        self.assert_valid_response_object(response)
        self.assertFalse(response['success'])
        self.assertIn('Failed Request', response['error'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_style(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.return_value = self.mock_styles[0]

        # Do delete
        response = self.engine.delete_style(style_id=self.style_names[0])

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response['success'])
        self.assertIsNone(response['result'])

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=None)
        mc.delete.assert_called_with(config_object=self.mock_styles[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_style_failed_request(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Do delete
        response = self.engine.delete_style(style_id=self.style_names[0])

        # Failure Check
        self.assert_valid_response_object(response)
        self.assertFalse(response['success'])
        self.assertIn('Failed Request', response['error'])

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_layer_group(self, mock_catalog):
        mc = mock_catalog()
        mc.create_layergroup.return_value = self.mock_layer_groups[0]

        # Do create
        expected_layer_group_id = self.mock_layer_groups[0]
        response = self.engine.create_layer_group(layer_group_id=expected_layer_group_id,
                                                  layers=self.layer_names, styles=self.style_names)
        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response['success'])

        # Validate
        result = response['result']
        self.assertEqual(result['name'], self.layer_group_names[0])
        self.assertEqual(result['layers'], self.layer_names)
        self.assertEqual(result['style'], self.style_names)

        # Clean up
        self.engine.delete_layer_group(layer_group_id=expected_layer_group_id)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_layer_group_conflict_error(self, mock_catalog):
        mc = mock_catalog()
        mc.create_layergroup.return_value = self.mock_layer_groups[0]
        mc.create_layergroup.side_effect = geoserver.catalog.ConflictingDataError('Conflicting Data Error')

        # Do create
        expected_layer_group_id = self.mock_layer_groups[0]
        response = self.engine.create_layer_group(layer_group_id=expected_layer_group_id,
                                                  layers=self.layer_names, styles=self.style_names)

        # False
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        # Check Properties
        self.assertIn('Conflicting Data Error', r)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_layer_group_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.create_layergroup.return_value = self.mock_layer_groups[0]
        mc.create_layergroup.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Do create
        expected_layer_group_id = self.mock_layer_groups[0]
        response = self.engine.create_layer_group(layer_group_id=expected_layer_group_id,
                                                  layers=self.layer_names, styles=self.style_names)

        # False
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        # Check Properties
        self.assertIn('Failed Request', r)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_default_workspace().name = self.workspace_name[0]
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_name = os.path.join(self.files_root, 'shapefile', 'test')
        store_id = self.store_names[0]

        # Execute
        response = self.engine.create_shapefile_resource(store_id=store_id,
                                                         shapefile_base=shapefile_name,
                                                         overwrite=True
                                                         )
        # Should succeed
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r['name'])
        self.assertIn(self.store_name[0], r['store'])

        mc.get_default_workspace.assert_called_with()
        mc.get_resource.assert_called_with(name=self.store_names[0], store=self.store_names[0],
                                           workspace=self.workspace_name[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource_zipfile(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_name = os.path.join(self.files_root, 'shapefile', 'test1.zip')
        # Workspace is given
        store_id = '{}:{}'.format(self.workspace_name, self.store_names[0])

        # Execute
        response = self.engine.create_shapefile_resource(store_id=store_id,
                                                         shapefile_zip=shapefile_name,
                                                         overwrite=True,
                                                         charset='ISO - 8559 - 1',
                                                         )
        # Should succeed
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r['name'])
        self.assertIn(self.store_name[0], r['store'])

        mc.get_resource.assert_called_with(name='test1', store=self.store_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource_upload(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_cst = os.path.join(self.files_root, 'shapefile', 'test.cst')
        shapefile_dbf = os.path.join(self.files_root, 'shapefile', 'test.dbf')
        shapefile_prj = os.path.join(self.files_root, 'shapefile', 'test.prj')
        shapefile_shp = os.path.join(self.files_root, 'shapefile', 'test.shp')
        shapefile_shx = os.path.join(self.files_root, 'shapefile', 'test.shx')

        # Workspace is given
        store_id = '{}:{}'.format(self.workspace_name, self.store_names[0])

        with open(shapefile_cst, 'rb') as cst_upload,\
                open(shapefile_dbf, 'rb') as dbf_upload,\
                open(shapefile_prj, 'rb') as prj_upload,\
                open(shapefile_shp, 'rb') as shp_upload,\
                open(shapefile_shx, 'rb') as shx_upload:
            upload_list = [cst_upload, dbf_upload, prj_upload, shp_upload, shx_upload]
            response = self.engine.create_shapefile_resource(store_id=store_id,
                                                             shapefile_upload=upload_list,
                                                             overwrite=True,
                                                             )
        # Should succeed
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r['name'])
        self.assertIn(self.store_name[0], r['store'])

        mc.get_resource.assert_called_with(name=self.store_names[0], store=self.store_names[0],
                                           workspace=self.workspace_name)

    def test_create_shapefile_resource_zipfile_typeerror(self):
        # Setup
        shapefile_name = os.path.join(self.files_root, 'shapefile', 'test.shp')
        # Workspace is given
        store_id = '{}:{}'.format(self.workspace_name, self.store_name[0])

        # Should Fail
        self.assertRaises(TypeError,
                          self.engine.create_shapefile_resource,
                          store_id=store_id,
                          shapefile_zip=shapefile_name,
                          overwrite=True)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource_overwrite_store_exists(self, mock_catalog):
        # Setup
        shapefile_name = os.path.join(self.files_root, 'shapefile', 'test')
        store_id = '{}:{}'.format(self.workspace_name, self.store_names[0])

        # Execute
        response = self.engine.create_shapefile_resource(store_id=store_id,
                                                         shapefile_base=shapefile_name,
                                                         overwrite=False
                                                         )
        # Should Fail
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        # Check error message
        error_message = 'There is already a store named ' + self.store_names[0] + ' in ' + self.workspace_name
        self.assertIn(error_message, r)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource_overwrite_store_not_exists(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_name = os.path.join(self.files_root, 'shapefile', 'test')
        # Workspace is given
        store_id = '{}:{}'.format(self.workspace_name, self.store_names[0])

        # Execute
        response = self.engine.create_shapefile_resource(store_id=store_id,
                                                         shapefile_base=shapefile_name,
                                                         overwrite=False
                                                         )
        # Should succeed
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r['name'])
        self.assertIn(self.store_name[0], r['store'])

        mc.get_resource.assert_called_with(name=self.store_names[0], store=self.store_names[0],
                                           workspace=self.workspace_name)

    def test_create_shapefile_resource_validate_shapefile_args(self):
        self.assertRaises(ValueError,
                          self.engine.create_shapefile_resource,
                          store_id='foo')
        self.assertRaises(ValueError,
                          self.engine.create_shapefile_resource,
                          store_id='foo',
                          shapefile_zip='zipfile',
                          shapefile_upload='su',
                          shapefile_base='base')
        self.assertRaises(ValueError,
                          self.engine.create_shapefile_resource,
                          store_id='foo',
                          shapefile_upload='su',
                          shapefile_base='base')
        self.assertRaises(ValueError,
                          self.engine.create_shapefile_resource,
                          store_id='foo',
                          shapefile_zip='zipfile',
                          shapefile_base='base')
        self.assertRaises(ValueError,
                          self.engine.create_shapefile_resource,
                          store_id='foo',
                          shapefile_zip='zipfile',
                          shapefile_upload='su')

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource_failure(self, _, mock_put):
        mock_put.return_value = MockResponse(404, reason='Failure')

        # Setup
        shapefile_name = os.path.join(self.files_root, 'shapefile', 'test')
        store_id = '{}:{}'.format(self.workspace_name, self.store_name[0])

        # Execute
        response = self.engine.create_shapefile_resource(store_id=store_id,
                                                         shapefile_base=shapefile_name,
                                                         overwrite=True
                                                         )
        # Should succeed
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        # Check Response
        self.assertIn('404', r)
        self.assertIn('Failure', r)

    def test_type_property(self):
        response = self.engine.type
        expected_response = 'GEOSERVER'

        # Check Response
        self.assertEqual(response, expected_response)

    def test_gwc_endpoint_property(self):
        response = self.engine.gwc_endpoint

        # Check Response
        self.assertIn('/gwc/rest/', response)

    def test_ini_no_slash_endpoint(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint='http://localhost:8181/geoserver/rest',
            username=TEST_GEOSERVER_DATASET_SERVICE['USERNAME'],
            password=TEST_GEOSERVER_DATASET_SERVICE['PASSWORD']
        )

        expected_endpoint = 'http://localhost:8181/geoserver/gwc/rest/'

        # Check Response
        self.assertEqual(expected_endpoint, self.engine.gwc_endpoint)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.get')
    def test_validate(self, mock_get):
        # Missing Schema
        mock_get.side_effect = requests.exceptions.MissingSchema
        self.assertRaises(AssertionError,
                          self.engine.validate
                          )

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.get')
    def test_validate_401(self, mock_get):
        # 401 Code
        mock_get.return_value = MockResponse(401)
        self.assertRaises(AssertionError,
                          self.engine.validate
                          )

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.get')
    def test_validate_not_200(self, mock_get):
        # !201 Code
        mock_get.return_value = MockResponse(201)

        self.assertRaises(AssertionError,
                          self.engine.validate
                          )

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.get')
    def test_validate_not_geoserver(self, mock_get):
        # text
        mock_get.return_value = MockResponse(200, text="Bad text")
        self.assertRaises(AssertionError,
                          self.engine.validate
                          )

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        mc = mock_catalog()
        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=True,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])

        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "image/geotiff",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_upload_zip_file(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'arcgrid'
        coverage_file_name = 'precip30min.zip'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, "arc_sample", coverage_file_name)

        mc = mock_catalog()
        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        with open(coverage_file, 'rb') as coverage_upload:
            # Execute
            response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                            coverage_type=expected_coverage_type,
                                                            coverage_upload=coverage_upload,
                                                            overwrite=True,
                                                            debug=False)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])

        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_upload_image(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        mc = mock_catalog()
        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        with open(coverage_file, 'rb') as coverage_upload:
            # Execute
            response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                            coverage_type=expected_coverage_type,
                                                            coverage_upload=coverage_upload,
                                                            overwrite=True,
                                                            debug=False)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])

        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "image/geotiff",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_upload_multiple_files_in_memory(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'arcgrid'
        coverage_file_name = 'precip30min.asc'
        prj_file_name = 'precip30min.prj'
        coverage_name = coverage_file_name.split('.')[0]
        arc_sample = os.path.join(self.files_root, "arc_sample")
        coverage_file = os.path.join(arc_sample, coverage_file_name)
        prj_file = os.path.join(arc_sample, prj_file_name)

        mc = mock_catalog()
        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        with open(coverage_file, 'rb') as coverage_upload:
            with open(prj_file, 'rb') as prj_upload:
                upload_list = [coverage_upload, prj_upload]

                # Execute
                response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                                coverage_type=expected_coverage_type,
                                                                coverage_upload=upload_list,
                                                                overwrite=True,
                                                                debug=False)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])

        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_no_overwrite_store_exists(self, _):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_file = os.path.join(self.files_root, coverage_file_name)
        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=False,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)
        # Success
        self.assertFalse(response['success'])
        # Extract Result
        r = response['error']

        # Type
        self.assertIsInstance(r, str)

        self.assertIn('There is already a store named', r)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_no_overwrite_store_not_exists(self, mock_catalog, mock_put):
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError('FailedRequest')

        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)
        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=False,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)
        # Success
        self.assertTrue(response['success'])
        # Extract Result
        r = response['result']
        # Type
        self.assertIsInstance(r, dict)
        # Values
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])
        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "image/geotiff",
            "Accept": "application/xml"
        }
        expected_params = {
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    def test_create_coverage_resource_invalid_coverage_type(self):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'test1'
        coverage_file_name = 'adem.tif'
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        # Raise ValueError
        self.assertRaises(ValueError, self.engine.create_coverage_resource, store_id=expected_store_id,
                          coverage_type=expected_coverage_type,
                          coverage_file=coverage_file,
                          overwrite=False,
                          debug=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_no_store_workspace(self, mock_catalog, mock_put):
        default_workspace_name = 'default-workspace'
        mock_default_workspace = mock.NonCallableMagicMock()
        mock_default_workspace.name = default_workspace_name
        mc = mock_catalog()
        mc.get_default_workspace().name = self.workspace_name

        expected_store_id = self.store_names[0]
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_name)
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=True,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_name, r['workspace'])
        mc.get_default_workspace.assert_called()
        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_name)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_name,
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "image/geotiff",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resources_zip_file(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'arcgrid'
        coverage_file_name = 'precip30min.zip'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, "arc_sample", coverage_file_name)

        mc = mock_catalog()
        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=True,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Tests
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])

        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_not_query_after_success(self, _, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, coverage_file_name)
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=True,
                                                        debug=False,
                                                        query_after_success=False)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsNone(r)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "image/geotiff",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_not_201(self, _, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        mock_put.return_value = MockResponse(401)

        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=True,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext=expected_coverage_type
        )
        expected_headers = {
            "Content-type": "image/geotiff",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_grass_grid(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'grassgrid'
        coverage_file_name = 'my_grass.zip'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        mc = mock_catalog()
        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=True,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Tests
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])

        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext='arcgrid'
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_grass_grid_existing_folder(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'grassgrid'
        coverage_file_name = 'my_grass.zip'
        coverage_name = coverage_file_name.split('.')[0]
        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        mc = mock_catalog()
        mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_resource.name = coverage_name
        mc.get_resource.return_value = mock_resource
        mock_put.return_value = MockResponse(201)

        # Creating temp folder
        working_dir = os.path.join(os.path.dirname(coverage_file), '.gstmp')
        os.makedirs(working_dir)

        # Execute
        response = self.engine.create_coverage_resource(store_id=expected_store_id,
                                                        coverage_type=expected_coverage_type,
                                                        coverage_file=coverage_file,
                                                        overwrite=True,
                                                        debug=False)
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        # Tests
        self.assertEqual(coverage_name, r['name'])
        self.assertEqual(self.workspace_names[0], r['workspace'])

        mc.get_resource.assert_called_with(name=coverage_name, store=self.store_names[0],
                                           workspace=self.workspace_names[0])

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0],
            ext='arcgrid'
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml"
        }
        expected_params = {
            'update': 'overwrite',
            'coverageName': coverage_name
        }
        self.assertEqual(expected_url, put_call_args[0][1]['url'])
        self.assertEqual(expected_headers, put_call_args[0][1]['headers'])
        self.assertEqual(expected_params, put_call_args[0][1]['params'])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_grass_grid_invalid_file(self, _):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'grassgrid'
        coverage_file_name = 'my_grass_invalid.zip'
        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        # Execute
        self.assertRaises(IOError, self.engine.create_coverage_resource,
                          store_id=expected_store_id,
                          coverage_type=expected_coverage_type,
                          coverage_file=coverage_file,
                          overwrite=True,
                          debug=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_grass_grid_no_coverage_file(self, _):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'grassgrid'

        # Execute
        self.assertRaises(ValueError, self.engine.create_coverage_resource,
                          store_id=expected_store_id,
                          coverage_type=expected_coverage_type,
                          coverage_file=None,
                          overwrite=True,
                          debug=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_grass_grid_coverage_file_not_zip(self, _):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'grassgrid'
        coverage_file_name = 'my_grass.asc'
        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        # Execute
        self.assertRaises(ValueError, self.engine.create_coverage_resource,
                          store_id=expected_store_id,
                          coverage_type=expected_coverage_type,
                          coverage_file=coverage_file,
                          overwrite=True,
                          debug=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_workspace(self, mock_catalog):
        mc = mock_catalog()
        expected_uri = 'http:www.example.com/b-workspace'

        mc.create_workspace.return_value = self.mock_workspaces[0]

        # Execute
        response = self.engine.create_workspace(workspace_id=self.workspace_names[0],
                                                uri=expected_uri)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        self.assertIn('name', r)
        self.assertEqual(self.workspace_names[0], r['name'])

        mc.create_workspace.assert_called_with(self.workspace_names[0], expected_uri)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_workspace_assertion_error(self, mock_catalog):
        mc = mock_catalog()
        expected_uri = 'http:www.example.com/b-workspace'
        mc.create_workspace.side_effect = AssertionError('AssertionError')

        # Execute
        response = self.engine.create_workspace(workspace_id=self.workspace_names[0],
                                                uri=expected_uri)
        # False
        self.assertFalse(response['success'])
        # Expect Error
        r = response['error']
        # Properties
        self.assertIn('AssertionError', r)
        mc.create_workspace.assert_called_with(self.workspace_names[0], expected_uri)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_style(self, mock_catalog):
        expected_style_id = 'style1'
        expected_sld = 'Style one test'

        mc = mock_catalog()

        # Execute
        response = self.engine.create_style(style_id=expected_style_id, sld=expected_sld)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        mc.create_style.assert_called_with(name=expected_style_id, data=expected_sld, workspace=None, overwrite=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_style_assertion_error(self, mock_catalog):
        mc = mock_catalog()
        expected_style_id = 'style1'
        expected_sld = 'Style one test'
        mc.create_style.side_effect = AssertionError('Upload error')

        # Execute
        response = self.engine.create_style(style_id=expected_style_id, sld=expected_sld)

        # False
        self.assertFalse(response['success'])
        # Expect Error
        r = response['error']
        # Properties
        self.assertIn('Upload error', r)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_style_conflicting_data_error(self, mock_catalog):
        mc = mock_catalog()
        expected_style_id = 'style1'
        expected_sld = 'Style one test'
        mc.create_style.side_effect = geoserver.catalog.ConflictingDataError('Conflictingdata error')

        # Execute
        response = self.engine.create_style(style_id=expected_style_id, sld=expected_sld)

        # False
        self.assertFalse(response['success'])
        # Expect Error
        r = response['error']
        # Properties
        self.assertIn('Conflictingdata error', r)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_style_upload_error(self, mock_catalog):
        mc = mock_catalog()
        mc.create_style.side_effect = geoserver.catalog.UploadError()
        expected_style_id = 'style1'
        expected_sld = 'Style one test'

        # Should Fail
        self.assertRaises(geoserver.catalog.UploadError,
                          self.engine.create_style,
                          style_id=expected_style_id,
                          sld=expected_sld)

        mc.create_style.assert_called_with(name=expected_style_id, data=expected_sld, workspace=None, overwrite=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_style_upload_error_recover(self, mock_catalog):
        mc = mock_catalog()
        mc.create_style.side_effect = self.mock_upload_fail_three_times
        expected_style_id = 'style1'
        expected_sld = 'Style one test'

        # Should Fail
        response = self.engine.create_style(style_id=expected_style_id, sld=expected_sld)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        mc.create_style.assert_called_with(name=expected_style_id, data=expected_sld, workspace=None, overwrite=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTable')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTableGeometry')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTableParam')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_sql_view(self, mock_catalog, _, __, ___):

        mc = mock_catalog()

        feature_type_name = 'foo'
        sql_input = 'Select * from pipes'
        geometry_column = 'geometry'
        geometry_type = 'LineString'
        geometry_srid = 4326

        expected_postgis_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])

        mc.get_store.return_value = self.mock_stores[0]
        mock_layer = mock.NonCallableMagicMock()
        mock_layer.name = feature_type_name
        mc.get_layer.return_value = mock_layer

        response = self.engine.create_sql_view(feature_type_name=feature_type_name,
                                               postgis_store_id=expected_postgis_store_id,
                                               sql=sql_input,
                                               geometry_column=geometry_column,
                                               geometry_type=geometry_type,
                                               geometry_srid=geometry_srid)

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        self.assertIn('name', r)
        self.assertEqual(feature_type_name, r['name'])

        mc.get_store.assert_called_with(self.store_names[0], workspace=self.workspace_names[0])
        mc.get_layer.assert_called_with(feature_type_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTable')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTableGeometry')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTableParam')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_sql_view_default_style_id(self, mock_catalog, _, __, ___):

        mc = mock_catalog()

        feature_type_name = 'foo'
        sql_input = 'Select * from pipes'
        geometry_column = 'geometry'
        geometry_type = 'LineString'
        geometry_srid = 4326
        default_style_id = '{}:{}'.format(self.workspace_names[0], 'pipes')

        expected_postgis_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])

        mc.get_store.return_value = self.mock_stores[0]
        mock_layer = mock.NonCallableMagicMock()
        mock_layer.name = feature_type_name
        mock_layer.default_style = ''
        mc.get_layer.return_value = mock_layer
        mock_style = mock.NonCallableMagicMock(workspace=self.workspace_names[0])
        mock_style.name = 'pipes'
        mc.get_style.return_value = mock_style

        response = self.engine.create_sql_view(feature_type_name=feature_type_name,
                                               postgis_store_id=expected_postgis_store_id,
                                               sql=sql_input,
                                               geometry_column=geometry_column,
                                               geometry_type=geometry_type,
                                               geometry_srid=geometry_srid,
                                               default_style_id=default_style_id)

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        self.assertIn('name', r)
        self.assertEqual(feature_type_name, r['name'])
        self.assertIn('default_style', r)
        self.assertEqual(default_style_id, r['default_style'])

        mc.get_store.assert_called_with(self.store_names[0], workspace=self.workspace_names[0])
        mc.get_layer.assert_called_with(feature_type_name)
        mc.get_style.assert_called_with('pipes', workspace=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTable')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTableGeometry')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.JDBCVirtualTableParam')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_sql_view_with_parameters(self, mock_catalog, _, __, ____):
        mc = mock_catalog()
        feature_type_name = 'foo'
        sql_input = 'Select * from pipes'
        geometry_column = 'geometry'
        geometry_type = 'LineString'
        geometry_srid = 4326
        default_style_id = '{}:{}'.format(self.workspace_names[0], 'pipes')
        parameters = [('column1', 'pressure', '>100'), ('column2', 'temperature', '<20')]

        expected_postgis_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])

        mc.get_store.return_value = self.mock_stores[0]
        mock_layer = mock.NonCallableMagicMock()
        mock_layer.name = feature_type_name
        mc.get_layer.return_value = mock_layer

        response = self.engine.create_sql_view(feature_type_name=feature_type_name,
                                               postgis_store_id=expected_postgis_store_id,
                                               sql=sql_input,
                                               geometry_column=geometry_column,
                                               geometry_type=geometry_type,
                                               geometry_srid=geometry_srid,
                                               default_style_id=default_style_id,
                                               parameters=parameters)

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn('name', r)
        self.assertEqual(feature_type_name, r['name'])

        mc.get_store.assert_called_with(self.store_names[0], workspace=self.workspace_names[0])

        mc.get_layer.assert_called_with(feature_type_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_apply_changes_to_gs_object(self, mock_catalog):
        mc = mock_catalog()
        gs_object = mock.NonCallableMagicMock(
            layer_id=self.layer_names[0],
            styles=self.style_names,
            default_style='d_styles'
        )
        # new style
        new_gs_args = {'styles': ['style1:style1a', 'style2'], 'default_style': 'dstyle1'}

        # mock get_style to return value
        mc.get_style.return_value = self.mock_styles[0]

        # Execute
        new_gs_object = self.engine._apply_changes_to_gs_object(new_gs_args, gs_object)

        style = new_gs_object.styles[0].name
        d_style = new_gs_object.default_style.name

        # validate
        self.assertIn(self.mock_styles[0].name, style)
        self.assertIn(self.mock_styles[0].name, d_style)

        # test default case with :
        new_gs_args = {'default_style': 'dstyle1: dstyle2'}

        # mock get_style to return value
        mc.get_style.return_value = self.mock_styles[0]

        # Execute
        new_gs_object = self.engine._apply_changes_to_gs_object(new_gs_args, gs_object)

        d_style = new_gs_object.default_style.name

        # validate
        self.assertIn(self.mock_styles[0].name, d_style)

    def test_get_non_rest_endpoint(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint='http://localhost:8181/geoserver/rest/',
        )

        expected_endpoint = 'http://localhost:8181/geoserver'
        endpoint = self.engine._get_non_rest_endpoint()

        # Check Response
        self.assertEqual(expected_endpoint, endpoint)

    def test_get_wms_url(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint='http://localhost:8181/geoserver/rest/',
        )

        # tiled and transparent are set as default value
        wms_url = self.engine._get_wms_url(layer_id=self.layer_names[0],
                                           style=self.style_names[0],
                                           srs='EPSG:4326',
                                           bbox='-180,-90,180,90',
                                           version='1.1.0',
                                           width='512',
                                           height='512',
                                           output_format='image/png',
                                           tiled=False, transparent=True)

        expected_url = 'http://localhost:8181/geoserver/wms?service=WMS&version=1.1.0&' \
                       'request=GetMap&layers={0}&styles={1}&transparent=true&' \
                       'tiled=no&srs=EPSG:4326&bbox=-180,-90,180,90&' \
                       'width=512&height=512&format=image/png'.format(self.layer_names[0], self.style_names[0])

        # check wms_url
        self.assertEqual(expected_url, wms_url)

        # tiled and transparent are set as default value
        wms_url = self.engine._get_wms_url(layer_id=self.layer_names[0],
                                           style=self.style_names[0],
                                           srs='EPSG:4326',
                                           bbox='-180,-90,180,90',
                                           version='1.1.0',
                                           width='512',
                                           height='512',
                                           output_format='image/png',
                                           tiled=True, transparent=False)

        expected_url = 'http://localhost:8181/geoserver/wms?service=WMS&version=1.1.0&' \
                       'request=GetMap&layers={0}&styles={1}&transparent=false&' \
                       'tiled=yes&srs=EPSG:4326&bbox=-180,-90,180,90&' \
                       'width=512&height=512&format=image/png'.format(self.layer_names[0], self.style_names[0])

        # check wms_url
        self.assertEqual(expected_url, wms_url)

    def test_get_wcs_url(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint='http://localhost:8181/geoserver/rest/',
        )

        wcs_url = self.engine._get_wcs_url(resource_id=self.resource_names[0],
                                           srs='EPSG:4326', bbox='-180,-90,180,90',
                                           output_format='png', namespace=self.store_name,
                                           width='512', height='512')

        expected_wcs_url = 'http://localhost:8181/geoserver/wcs?service=WCS&version=1.1.0&' \
                           'request=GetCoverage&identifier={0}&srs=EPSG:4326&' \
                           'BoundingBox=-180,-90,180,90&width=512&' \
                           'height=512&format=png&namespace={1}'.format(self.resource_names[0], self.store_name)

        # check wcs_url
        self.assertEqual(expected_wcs_url, wcs_url)

    def test_get_wfs_url(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint='http://localhost:8181/geoserver/rest/',
        )

        # GML3 Case
        wfs_url = self.engine._get_wfs_url(resource_id=self.resource_names[0], output_format='GML3')
        expected_wfs_url = 'http://localhost:8181/geoserver/wfs?service=WFS&' \
                           'version=2.0.0&request=GetFeature&' \
                           'typeNames={0}'.format(self.resource_names[0])
        # check wcs_url
        self.assertEqual(expected_wfs_url, wfs_url)

        # GML2 Case
        wfs_url = self.engine._get_wfs_url(resource_id=self.resource_names[0], output_format='GML2')
        expected_wfs_url = 'http://localhost:8181/geoserver/wfs?service=WFS&' \
                           'version=1.0.0&request=GetFeature&' \
                           'typeNames={0}&outputFormat={1}'.format(self.resource_names[0], 'GML2')
        # check wcs_url
        self.assertEqual(expected_wfs_url, wfs_url)

        # Other format Case
        wfs_url = self.engine._get_wfs_url(resource_id=self.resource_names[0], output_format='Other')
        expected_wfs_url = 'http://localhost:8181/geoserver/wfs?service=WFS&' \
                           'version=2.0.0&request=GetFeature&' \
                           'typeNames={0}&outputFormat={1}'.format(self.resource_names[0], 'Other')
        # check wcs_url
        self.assertEqual(expected_wfs_url, wfs_url)

    @mock.patch('sys.stdout', new_callable=StringIO)
    def test_handle_debug(self, mock_print):
        test_object = self.style_names

        self.engine._handle_debug(test_object, debug=True)

        output = mock_print.getvalue()

        # check results
        self.assertIn(self.style_names[0], output)

    def test_transcribe_geoserver_object(self):

        # NAMED_OBJECTS
        gs_object_store = mock.NonCallableMagicMock(
            store=self.store_name,
            styles=self.style_names
        )
        store_dict = self.engine._transcribe_geoserver_object(gs_object_store)

        # check if type is dic
        self.assertIsInstance(store_dict, dict)

        # check properties
        self.assertIn(self.store_name, store_dict['store'])
        self.assertIn(self.style_names[0], store_dict['styles'])

        # NAMED_OBJECTS_WITH_WORKSPACE
        gs_sub_object_resource = mock.NonCallableMagicMock(workspace=self.workspace_name,
                                                           writers='test_omit_attributes')
        gs_sub_object_resource.name = self.resource_names[0]
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            default_style=self.default_style_name,
        )
        resource_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        # check if type is dic
        self.assertIsInstance(resource_dict, dict)

        # check properties
        resource_att = '{0}:{1}'.format(self.workspace_name, self.resource_names[0])
        self.assertIn(resource_att, resource_dict['resource'])
        self.assertIn(self.default_style_name, resource_dict['default_style'])

        # NAMED_OBJECTS_WITH_NO_WORKSPACE to Cover if sub_object.workspace is not true
        gs_sub_object_resource = mock.NonCallableMagicMock(workspace=None)
        gs_sub_object_resource.name = self.resource_names[0]
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            default_style=self.default_style_name,
        )
        resource_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        # check if type is dic
        self.assertIsInstance(resource_dict, dict)

        # check properties
        resource_att = self.resource_names[0]
        self.assertIn(resource_att, resource_dict['resource'])
        self.assertIn(self.default_style_name, resource_dict['default_style'])

        # resource_type with workspace
        gs_object_resource = mock.NonCallableMagicMock(
            resource_type='featureType',
            workspace=self.workspace_name,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        self.assertIn('gml3', resource_type_dict['wfs'])

        # resource_type with no workspace
        gs_object_resource = mock.NonCallableMagicMock(
            resource_type='featureType',
            workspace=None,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        self.assertIn('gml3', resource_type_dict['wfs'])

        # resource_type with no workspace and coverage
        gs_sub_object_resource = mock.NonCallableMagicMock(native_bbox=['0', '1', '2', '3'])
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            resource_type='coverage',
            workspace=None,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        self.assertIn('png', resource_type_dict['wcs'])

        # resource_type with workspace and coverage -wcs
        gs_sub_object_resource = mock.NonCallableMagicMock(native_bbox=['0', '1', '2', '3'])
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            resource_type='coverage',
            workspace=self.workspace_name,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        self.assertIn('png', resource_type_dict['wcs'])

        # resource_type with workspace and layer - wms
        gs_sub_object_resource = mock.NonCallableMagicMock(native_bbox=['0', '1', '2', '3'])
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            resource_type='layer',
            workspace=self.workspace_name,
            default_style=self.default_style_name
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        self.assertIn('png', resource_type_dict['wms'])

        # resource_type with workspace and layer - wms with bounds
        gs_sub_object_resource = mock.NonCallableMagicMock(native_bbox=['0', '1', '2', '3'])
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            bounds=['0', '1', '2', '3', '4'],
            resource_type='layerGroup',
            workspace=self.workspace_name,
            default_style=self.default_style_name
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        self.assertIn('png', resource_type_dict['wms'])

    def test_link_sqlalchemy_db_to_geoserver(self):
        self.engine.create_postgis_feature_resource = mock.MagicMock()
        url = 'postgresql://user:pass@localhost:5432/foo'
        engine = create_engine(url)
        self.engine.link_sqlalchemy_db_to_geoserver(store_id=self.store_names[0], sqlalchemy_engine=engine)
        self.engine.create_postgis_feature_resource.assert_called_with(store_id=self.store_names[0],
                                                                       host='localhost',
                                                                       port=5432,
                                                                       database='foo',
                                                                       user='user',
                                                                       password='pass',
                                                                       debug=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_postgis_feature_resource(self, mock_catalog, mock_post):
        store_id = self.store_names[0]
        host = 'localhost'
        port = 5432
        database = 'foo'
        user = 'user'
        password = 'pass'
        table_name = 'points'

        mc = mock_catalog()
        mc.get_store.return_value = self.mock_store[0]
        # First get resource mock call needs to return None to pass the checking existing resource check
        # Second get resource mock call return NonMockableObject
        mc.get_resource.side_effect = (geoserver.catalog.FailedRequestError,
                                       mock_get_resource(name=table_name, store=store_id,
                                                         workspace=self.mock_workspaces[0]))

        mc.get_default_workspace.return_value = self.mock_workspaces[0]
        mock_post.return_value = MockResponse(201)

        response = self.engine.create_postgis_feature_resource(store_id=store_id,
                                                               host=host,
                                                               port=port,
                                                               database=database,
                                                               user=user,
                                                               password=password,
                                                               table=table_name,
                                                               debug=False)

        expected_url = '{endpoint}workspaces/{w}/datastores/{s}/featuretypes'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0]
        )
        expected_headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn('name', r)
        self.assertIn(table_name, r['name'])
        self.assertIn('store', r)
        self.assertEqual(self.store_names[0], r['store'])

        post_call_args = mock_post.call_args_list
        self.assertEqual(expected_url, post_call_args[0][1]['url'])
        self.assertEqual(expected_headers, post_call_args[0][1]['headers'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.mock_workspaces[0].name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_postgis_feature_resource_no_store(self, mock_catalog, mock_post):
        mc = mock_catalog()
        store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        host = 'localhost'
        port = 5432
        database = 'foo'
        user = 'user'
        password = 'pass'
        table_name = 'points'

        mc.get_store.return_value = None
        mc.get_resource.side_effect = (geoserver.catalog.FailedRequestError,
                                       mock_get_resource(name=table_name, store=store_id,
                                                         workspace=self.mock_workspaces[0]))
        mock_post.return_value = MockResponse(201)

        response = self.engine.create_postgis_feature_resource(store_id=store_id,
                                                               host=host,
                                                               port=port,
                                                               database=database,
                                                               user=user,
                                                               password=password,
                                                               table=table_name,
                                                               debug=False)

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']
        # Type
        self.assertIn(table_name, r['name'])
        self.assertIn('store', r)
        self.assertIn(self.store_names[0], r['store'])

        expected_headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        post_call_args = mock_post.call_args_list

        # Execute: POST /workspaces/<ws>/datastores
        expected_url = '{endpoint}workspaces/{w}/datastores'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
        )
        self.assertEqual(expected_url, post_call_args[0][1]['url'])
        self.assertEqual(expected_headers, post_call_args[0][1]['headers'])

        # Execute: POST /workspaces/<ws>/datastores/<ds>/featuretypes
        expected_url = '{endpoint}workspaces/{w}/datastores/{s}/featuretypes'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0]
        )
        self.assertEqual(expected_url, post_call_args[1][1]['url'])
        self.assertEqual(expected_headers, post_call_args[1][1]['headers'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_postgis_feature_resource_no_store_not_201(self, mock_catalog, mock_post):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]

        store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        host = 'localhost'
        port = 5432
        database = 'foo'
        user = 'user'
        password = 'pass'
        table_name = 'points'

        mc.get_store.return_value = None
        mc.get_resource.side_effect = (geoserver.catalog.FailedRequestError,
                                       mock_get_resource(name=table_name, store=store_id,
                                                         workspace=self.mock_workspaces[0]))
        mock_post.return_value = MockResponse(500)

        response = self.engine.create_postgis_feature_resource(store_id=store_id,
                                                               host=host,
                                                               port=port,
                                                               database=database,
                                                               user=user,
                                                               password=password,
                                                               table=table_name,
                                                               debug=False)

        self.assertFalse(response['success'])

        expected_headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        # Execute: POST /workspaces/<ws>/datastores
        expected_url = '{endpoint}workspaces/{w}/datastores'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
        )

        post_call_args = mock_post.call_args_list
        self.assertEqual(expected_url, post_call_args[0][1]['url'])
        self.assertEqual(expected_headers, post_call_args[0][1]['headers'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_postgis_feature_resource_table_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]

        store_id = '{}:{}'.format(self.workspace_name, self.store_names[0])
        host = 'localhost'
        port = 5432
        database = 'foo'
        user = 'user'
        password = 'pass'
        table_name = None

        response = self.engine.create_postgis_feature_resource(store_id=store_id,
                                                               host=host,
                                                               port=port,
                                                               database=database,
                                                               user=user,
                                                               password=password,
                                                               table=table_name,
                                                               debug=False)

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        self.assertIn('name', r)
        self.assertIn(self.store_names[0], r['name'])
        self.assertIn('workspace', r)
        self.assertEqual(self.workspace_name, r['workspace'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.time')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_postgis_feature_resource_table_none_no_store(self, mock_catalog, mock_post, mock_time):
        mc = mock_catalog()
        mc.get_store.return_value = None

        mock_post.return_value = MockResponse(201)

        store_id = '{}:{}'.format(self.workspace_name, self.store_names[0])
        host = 'localhost'
        port = 5432
        database = 'foo'
        user = 'user'
        password = 'pass'
        table_name = None

        response = self.engine.create_postgis_feature_resource(store_id=store_id,
                                                               host=host,
                                                               port=port,
                                                               database=database,
                                                               user=user,
                                                               password=password,
                                                               table=table_name,
                                                               debug=False)

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertEqual({}, r)

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_name)
        mock_time.sleep.assert_called_with(1)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_postgis_feature_resource_not_201(self, mock_catalog, mock_post):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]

        store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        host = 'localhost'
        port = 5432
        database = 'foo'
        user = 'user'
        password = 'pass'
        table_name = 'points'

        mc.get_resource.side_effect = mock_get_resource_create_postgis_feature_resource

        mock_post.return_value = MockResponse(500)

        response = self.engine.create_postgis_feature_resource(store_id=store_id,
                                                               host=host,
                                                               port=port,
                                                               database=database,
                                                               user=user,
                                                               password=password,
                                                               table=table_name,
                                                               debug=False)

        expected_url = '{endpoint}workspaces/{w}/datastores/{s}/featuretypes'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0]
        )
        expected_headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        self.assertFalse(response['success'])

        post_call_args = mock_post.call_args_list
        self.assertEqual(expected_url, post_call_args[0][1]['url'])
        self.assertEqual(expected_headers, post_call_args[0][1]['headers'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_postgis_feature_resource_already_exist(self, mock_catalog, mock_post):
        mc = mock_catalog()
        store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        host = 'localhost'
        port = 5432
        database = 'foo'
        user = 'user'
        password = 'pass'
        table_name = 'points'

        mc.get_store.return_value = None
        mc.get_resource.side_effect = mock_get_resource
        mock_post.return_value = MockResponse(201)

        response = self.engine.create_postgis_feature_resource(store_id=store_id,
                                                               host=host,
                                                               port=port,
                                                               database=database,
                                                               user=user,
                                                               password=password,
                                                               table=table_name,
                                                               debug=False)

        self.assertFalse(response['success'])
        error_message = 'There is already a resource named {} in {}'.\
            format(table_name, self.workspace_names[0])

        # Type
        self.assertEqual(response['error'], error_message)

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_names[0])
        mc.get_resource.assert_called_with(name=table_name, store=self.store_names[0],
                                           workspace=self.mock_workspaces[0].name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_add_table_to_postgis_store(self, mock_catalog, mock_post):
        mc = mock_catalog()

        mc.get_store.return_value = self.mock_stores[0]
        mc.get_default_workspace.return_value = self.mock_workspaces[0]

        store_id = self.store_names[0]

        mock_post.return_value = MockResponse(201)

        table_name = 'points'

        response = self.engine.add_table_to_postgis_store(store_id=store_id,
                                                          table=table_name,
                                                          debug=False)

        expected_url = '{endpoint}workspaces/{w}/datastores/{s}/featuretypes'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0]
        )
        expected_headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        # Validate response object
        self.assert_valid_response_object(response)

        self.assertTrue(response['success'])

        # Extract Result
        r = response['result']

        # Type
        self.assertIsInstance(r, dict)

        self.assertIn('name', r)
        self.assertIn(self.store_names[0], r['name'])

        post_call_args = mock_post.call_args_list
        self.assertEqual(expected_url, post_call_args[0][1]['url'])
        self.assertEqual(expected_headers, post_call_args[0][1]['headers'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_add_table_to_postgis_store_fail_request(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError()
        store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])

        table_name = 'points'

        response = self.engine.add_table_to_postgis_store(store_id=store_id,
                                                          table=table_name,
                                                          debug=False)

        # Validate response object
        self.assert_valid_response_object(response)

        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('There is no store named', r)

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_add_table_to_postgis_store_not_201(self, mock_catalog, mock_post):
        mc = mock_catalog()

        mc.get_store.return_value = self.mock_stores[0]

        store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])

        mock_post.return_value = MockResponse(500)

        table_name = 'points'

        response = self.engine.add_table_to_postgis_store(store_id=store_id,
                                                          table=table_name,
                                                          debug=False)

        expected_url = '{endpoint}workspaces/{w}/datastores/{s}/featuretypes'.format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=self.store_names[0]
        )
        expected_headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        # Validate response object
        self.assert_valid_response_object(response)

        self.assertFalse(response['success'])

        post_call_args = mock_post.call_args_list
        self.assertEqual(expected_url, post_call_args[0][1]['url'])
        self.assertEqual(expected_headers, post_call_args[0][1]['headers'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=self.workspace_names[0])
