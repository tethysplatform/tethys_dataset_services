import os
import random
import string
import unittest
import mock
import geoserver

from tethys_dataset_services.engines import GeoServerSpatialDatasetEngine

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


def pause(seconds):
    # Pause
    for i in range(0, 10000 * seconds):
        pass


def mock_get_style(name, workspace=None):
    mock_style = mock.NonCallableMagicMock(workspace=workspace)
    mock_style.name = name
    return mock_style


class MockResponse(object):
    def __init__(self, status_code, text=None, json=None, reason=None):
        self.status_code = status_code
        self.text = text
        self.json_obj = json
        self.reason = reason

    def json(self):
        return self.json_obj

    def reason(self):
        return self.reason


class TestGeoServerDatasetEngine(unittest.TestCase):

    def setUp(self):
        # Globals
        self.debug = False

        # Files
        self.tests_root = os.path.abspath(os.path.dirname(__file__))
        self.files_root = os.path.join(self.tests_root, 'files')
        self.shapefile_name = 'test'
        self.shapefile_base = os.path.join(self.files_root, 'shapefile', self.shapefile_name)

        # Create Test Engine
        self.engine = GeoServerSpatialDatasetEngine(endpoint=TEST_GEOSERVER_DATASET_SERVICE['ENDPOINT'],
                                                    username=TEST_GEOSERVER_DATASET_SERVICE['USERNAME'],
                                                    password=TEST_GEOSERVER_DATASET_SERVICE['PASSWORD'])

        # Catalog
        self.catalog_endpoint = 'http://localhost:8181/geoserver/'
        self.mock_catalog = mock.NonCallableMagicMock(gs_base_url=self.catalog_endpoint)

        # Mock Objects
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
        self.mock_store_names = []
        for sn in self.store_names:
            mock_store_name = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_store_name.name = sn
            self.mock_store_names.append(mock_store_name)

        # # Create Test Workspaces
        # # self.test_resource_workspace = random_string_generator(10)
        # self.test_resource_workspace = random_string_generator(10)
        # self.engine.create_workspace(workspace_id=self.test_resource_workspace, uri=random_string_generator(5))
        #
        # # Create Test Stores/Resources/Layers
        # # Shapefile
        #
        # # Store name
        # self.test_resource_store = random_string_generator(10)
        #
        # # Resource and Layer will take the name of the file
        # self.test_resource_name = self.test_resource_store
        # self.test_layer_name = self.test_resource_store
        #
        # # Identifiers of the form 'workspace:item'
        # self.test_store_identifier = '{0}:{1}'.format(self.test_resource_workspace, self.test_resource_store)
        # self.test_resource_identifier = '{0}:{1}'.format(self.test_resource_workspace, self.test_resource_name)
        #
        # # Do create shapefile
        # self.engine.create_shapefile_resource(self.test_store_identifier, shapefile_base=self.shapefile_base,
        #                                       overwrite=True)
        #
        # # Coverage
        #
        # # Create Test Style
        # self.test_style_name = 'point'
        #
        # # Create Test Layer Groups
        # self.test_layer_group_name = random_string_generator(10)
        # self.engine.create_layer_group(layer_group_id=self.test_layer_group_name,
        #                                layers=(self.test_layer_name,),
        #                                styles=(self.test_style_name,))
        #
        # # Pause
        # pause(10)
        pass

    def tearDown(self):
        # # Delete test layer groups
        # self.engine.delete_layer_group(layer_group_id=self.test_layer_group_name)
        #
        # # Delete test resources & layers
        # self.engine.delete_resource(self.test_resource_identifier, recurse=True)
        #
        # # Delete stores
        # self.engine.delete_store(self.test_store_identifier)
        #
        # # Delete test workspace
        # self.engine.delete_workspace(self.test_resource_workspace)
        pass

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
        mc.get_stores.return_value = self.mock_store_names

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

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource_with_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

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

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

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

        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_resource_with_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Execute
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        response = self.engine.get_resource(resource_id=resource_id,
                                            store=self.store_name,
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

    def test_get_resource_multiple_with_name(self):
        raise NotImplementedError()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = self.mock_layers[0]

        # Execute
        response = self.engine.get_layer(layer_id=self.layer_names[0], debug=self.debug)

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

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_layer_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = None

        # Execute
        response = self.engine.get_layer(layer_id=self.layer_names[0], debug=self.debug)

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
        response = self.engine.get_layer(layer_id=self.layer_names[0], debug=self.debug)

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

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0])

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

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0])

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

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0])

    def test_get_store(self):
        raise NotImplementedError()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_style(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.return_value = self.mock_styles[0]

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

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_style_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.return_value = None

        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('not found', r)

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_style_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('Failed Request', r)

        mc.get_style.assert_called_with(name=self.style_names[0], workspace=None)

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

        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0])
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

        mc.get_layergroup.assert_called_with(name=self.mock_layer_groups[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_with_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        resource_id = '{}:{}'.format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)
        mc.delete.assert_called_with(config_object=self.mock_resources[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_without_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        resource_id = self.resource_names[0]

        # Execute
        response = self.engine.delete_resource(resource_id)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=None)
        mc.delete.assert_called_with(config_object=self.mock_resources[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]
        mc.delete.side_effect = geoserver.catalog.FailedRequestError()

        resource_id = '{}:{}'.format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])
        mc.delete.assert_called_with(config_object=self.mock_resources[0], purge=False, recurse=False)
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_resource_does_not_exist(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = None

        resource_id = '{}:{}'.format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])
        self.assertIn('GeoServer object does not exist', response['error'])
        mc.get_resource.assert_called_with(name=self.resource_names[0], store=None, workspace=self.workspace_name)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_layer(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = self.mock_layers[0]

        # Execute
        response = self.engine.delete_layer(self.layer_names[0])

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
        mc.get_layergroup.assert_called_with(name=self.layer_group_names[0])
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
        mc.get_store.return_value = self.mock_store_names[0]

        # Do delete
        response = self.engine.delete_store(store_id=self.store_names[0])

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response['success'])
        self.assertIsNone(response['result'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=None)
        mc.delete.assert_called_with(config_object=self.mock_store_names[0], purge=False, recurse=False)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_delete_store_failed_request(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Do delete
        response = self.engine.delete_store(store_id=self.store_names[0])

        # Failure Check
        self.assert_valid_response_object(response)
        self.assertFalse(response['success'])
        self.assertIs('Failed Request', response['error'])

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=None)

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
        self.assertIs('Failed Request', response['error'])

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        shapefile_name = os.path.join(dir_path, 'files', 'shapefile', 'test')
        store_id = self.store_name[0]

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
        mc.get_resource.assert_called_with(name=self.store_name[0], workspace=self.workspace_name[0])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource_zipfile(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        dir_path = os.path.dirname(os.path.realpath(__file__))
        shapefile_name = os.path.join(dir_path, 'files', 'shapefile', 'test1.zip')
        # Workspace is given
        store_id = '{}:{}'.format(self.workspace_name, self.store_name[0])

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

        mc.get_resource.assert_called_with(name=self.store_name[0], workspace=self.workspace_name)

    def test_create_shapefile_resource_zipfile_typeerror(self):
        # Setup
        dir_path = os.path.dirname(os.path.realpath(__file__))
        shapefile_name = os.path.join(dir_path, 'files', 'shapefile', 'test.shp')
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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        shapefile_name = os.path.join(dir_path, 'files', 'shapefile', 'test')
        store_id = '{}:{}'.format(self.workspace_name, self.store_name[0])

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
        error_message = 'There is already a store named ' + self.store_name[0] + ' in ' + self.workspace_name
        self.assertIn(error_message, r)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_shapefile_resource_overwrite_store_not_exists(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        dir_path = os.path.dirname(os.path.realpath(__file__))
        shapefile_name = os.path.join(dir_path, 'files', 'shapefile', 'test')
        # Workspace is given
        store_id = '{}:{}'.format(self.workspace_name, self.store_name[0])

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

        mc.get_resource.assert_called_with(name=self.store_name[0], workspace=self.workspace_name)

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        shapefile_name = os.path.join(dir_path, 'files', 'shapefile', 'test')
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
        expected_endpoint = 'http://localhost:8181/geoserver/gwc/rest/'

        # Check Response
        self.assertEqual(expected_endpoint, response)

    def test_ini_no_slash_endpoint(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint='http://localhost:8181/geoserver/rest',
            username=TEST_GEOSERVER_DATASET_SERVICE['USERNAME'],
            password=TEST_GEOSERVER_DATASET_SERVICE['PASSWORD']
        )

        expected_endpoint = 'http://localhost:8181/geoserver/gwc/rest/'

        # Check Response
        self.assertEqual(expected_endpoint, self.engine.gwc_endpoint)

    # @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.get')
    # def test_validate(self, mock_get):
    #     #401 Code
    #     mock_get.return_value = MockResponse('401')
    #
    #     # self.engine.validate()
    #     self.assertRaises(AssertionError,
    #                       self.engine.validate
    #                       )
    #
    #     #!201 Code
    #     mock_get.return_value = MockResponse('200')
    #
    #     # self.engine.validate()
    #     self.assertRaises(AssertionError,
    #                       self.engine.validate
    #                       )
    #
    #     #200 Code
    #     mock_get.return_value = MockResponse('200')
    #
    #     response = self.engine.validate()

    def test_create_coverage_resource(self):
        raise NotImplementedError()

    def test_create_workspace(self):
        raise NotImplementedError()

    def test_create_style(self):
        raise NotImplementedError()

    def test_create_sql_view(self):
        raise NotImplementedError()
