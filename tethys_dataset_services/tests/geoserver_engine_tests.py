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
    def test_get_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_store_names[0]

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

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_store_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = None

        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('not found', r)

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=None)

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_get_store_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError('Failed Request')

        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response['success'])

        # Extract Result
        r = response['error']

        self.assertIn('Failed Request', r)

        mc.get_store.assert_called_with(name=self.store_names[0], workspace=None)

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

    def test_delete_workspace(self):
        raise NotImplementedError()

    def test_delete_store(self):
        raise NotImplementedError()

    def test_delete_style(self):
        pass

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

        # Expect Error
        r = response['error']

        # Properties
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

        # Expect Error
        r = response['error']

        # Properties
        self.assertIn('Failed Request', r)

    # @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    # def test_create_shapefile_resource(self, mock_catalog):
    #     mc = mock_catalog()
    #     mc.create_shapefile_resource.return_value = mock.NonCallableMagicMock(
    #         store=self.store_name,
    #         title='foo',
    #         geometry='points'
    #     )
    #
    #     # Setup
    #     shapefile_name = random_string_generator(15)
    #     shapefile_base = shapefile_name + ".shp"
    #     shapefile_zip = shapefile_name + ".zip"
    #
    #     # Execute
    #     response = self.engine.create_shapefile_resource(store_id=self.store_name,
    #                                                      shapefile_base=shapefile_base,
    #                                                      shapefile_zip=shapefile_zip,
    #                                                      debug=self.debug)
    #
    #     print response

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.put')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource(self, mock_catalog, mock_put):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", coverage_file_name)

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

        mc.get_resource.assert_called_with(name=coverage_name, workspace=self.workspace_names[0])

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

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_create_coverage_resource_no_overwrite_store_exists(self, _):
        expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", coverage_file_name)
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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", coverage_file_name)

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
        mc.get_resource.assert_called_with(name=coverage_name, workspace=self.workspace_names[0])

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", coverage_file_name)

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
        mc.get_default_workspace.return_value = mock_default_workspace

        expected_store_id = self.store_names[0]
        expected_coverage_type = 'geotiff'
        coverage_file_name = 'adem.tif'
        coverage_name = coverage_file_name.split('.')[0]
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", coverage_file_name)

        mock_resource = mock.NonCallableMagicMock(workspace=default_workspace_name)
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
        self.assertEqual(default_workspace_name, r['workspace'])
        mc.get_default_workspace.assert_called()
        mc.get_resource.assert_called_with(name=coverage_name, workspace=default_workspace_name)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = '{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}'.format(
            endpoint=self.endpoint,
            w=default_workspace_name,
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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", "arc_sample", coverage_file_name)

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

        mc.get_resource.assert_called_with(name=coverage_name, workspace=self.workspace_names[0])

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", coverage_file_name)
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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", coverage_file_name)

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", "grass_ascii", coverage_file_name)

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

        mc.get_resource.assert_called_with(name=coverage_name, workspace=self.workspace_names[0])

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", "grass_ascii", coverage_file_name)

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        coverage_file = os.path.join(dir_path, "files", "grass_ascii", coverage_file_name)

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
    def test_create_sqlview(self, mock_catalog):
        pass
        # mc = mock_catalog()
        #
        # expected_store_id = '{}:{}'.format(self.workspace_names[0], self.store_names[0])
        #
        # store = self.engine.get_store(expected_store_id, workspace=self.workspace_names[0])
        #
        # epsg_code = 2246
        #
        # mock_geometry = "point"
        #
        # mc.JDBCVirtualTable.return_value = mock.NonCallableMagicMock(
        #     # feature_type_name=,
        #     # sql='foo',
        #     # geometry='points'
        # )
        #
