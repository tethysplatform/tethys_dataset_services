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
            mock_workspace = mock.NonCallableMagicMock(workspace=self.workspace_name)
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

    def test_get_resource_with_store(self):
        # Execute
        response = self.engine.get_resource(resource_id=self.test_resource_name,
                                            store=self.test_resource_store,
                                            debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('name', result)
        self.assertIn('store', result)
        self.assertEqual(result['name'], self.test_resource_name)
        self.assertEqual(result['store'], self.test_resource_store)

    def test_get_resource_multiple_with_name(self):
        raise NotImplementedError()

    def test_get_layer(self):
        # Execute
        response = self.engine.get_layer(layer_id=self.test_layer_name, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('name', result)
        self.assertEqual(result['name'], self.test_layer_name)

    def test_get_layer_group(self):
        # Execute
        response = self.engine.get_layer_group(layer_group_id=self.test_layer_group_name, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('name', result)
        self.assertEqual(result['name'], self.test_layer_group_name)

    def test_get_store(self):
        raise NotImplementedError()

    def test_get_workspace(self):
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

    def test_update_resource(self):
        # Setup
        new_title = random_string_generator(15)

        # Execute
        response = self.engine.update_resource(resource_id=self.test_resource_name,
                                               title=new_title,
                                               debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Get new resource
        updated_response = self.engine.get_resource(resource_id=self.test_resource_name, debug=self.debug)
        updated_result = updated_response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(updated_result['title'], new_title)
        self.assertEqual(result['title'], updated_result['title'])
        self.assertEqual(result, updated_result)

    def test_update_resource_workspace(self):
        # Setup
        new_title = random_string_generator(15)

        # Execute
        response = self.engine.update_resource(resource_id=self.test_resource_identifier,
                                               title=new_title,
                                               debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Get new resource
        updated_response = self.engine.get_resource(resource_id=self.test_resource_identifier,
                                                    debug=self.debug)
        updated_result = updated_response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(updated_result['title'], new_title)
        self.assertEqual(result['title'], updated_result['title'])
        self.assertEqual(result, updated_result)

    def test_update_resource_store(self):
        pause(10)
        # Setup
        new_title = random_string_generator(15)

        # Execute
        response = self.engine.update_resource(resource_id=self.test_resource_name,
                                               store=self.test_resource_store,
                                               title=new_title,
                                               debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Get new resource
        updated_response = self.engine.get_resource(resource_id=self.test_resource_name,
                                                    store=self.test_resource_store,
                                                    debug=self.debug)
        updated_result = updated_response['result']

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(updated_result['title'], new_title)
        self.assertEqual(result['title'], updated_result['title'])
        self.assertEqual(result, updated_result)

    def test_update_layer(self):
        # Get original
        old_response = self.engine.get_layer(layer_id=self.test_layer_name)

        # Update
        new_default_style = self.test_style_name
        response = self.engine.update_layer(layer_id=self.test_layer_name,
                                            default_style=new_default_style,
                                            debug=self.debug)

        # Update should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response['success'])

        old_result = old_response['result']
        result = response['result']
        self.assertEqual(result['default_style'], new_default_style)
        self.assertNotEqual(old_result['default_style'], result['default_style'])

    def test_update_layer_group(self):
        raise NotImplementedError()

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

    def test_create_shapefile_resource(self):
        raise NotImplementedError()

    def test_create_coverage_resource(self):
        raise NotImplementedError()

    def test_create_workspace(self):
        raise NotImplementedError()

    def test_create_style(self):
        raise NotImplementedError()

    def test_create_sql_view(self):
        raise NotImplementedError()
