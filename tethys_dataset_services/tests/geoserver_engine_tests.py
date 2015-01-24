import os
import random
import string
import unittest

from ..engines import GeoServerSpatialDatasetEngine

try:
    from .test_config import TEST_GEOSERVER_DATASET_SERVICE

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

        # Create Test Workspaces
        # self.test_resource_workspace = random_string_generator(10)
        self.test_resource_workspace = 'sf'

        # Create Test Stores/Resources/Layers
        ## Shapefile

        # Store name
        self.test_resource_store = random_string_generator(10)

        # Resource and Layer will take the name of the file
        self.test_resource_name = self.test_resource_store
        self.test_layer_name = self.test_resource_store

        # Identifiers of the form 'workspace:item'
        self.test_store_identifier = '{0}:{1}'.format(self.test_resource_workspace, self.test_resource_store)
        self.test_resource_identifier = '{0}:{1}'.format(self.test_resource_workspace, self.test_resource_name)

        # Do create shapefile
        self.engine.create_shapefile_resource(self.test_store_identifier, shapefile_base=self.shapefile_base,
                                              overwrite=True)

        ## Coverage

        # Create Test Layer Groups
        self.test_layer_group_name = 'tasmania'

        # Pause
        pause(20)

    def tearDown(self):
        # Delete test layer groups

        # Delete test resources & layers
        self.engine.delete_resource(self.test_resource_identifier, recurse=True)

        # Delete stores
        self.engine.delete_store(self.test_store_identifier)

        # Delete test workspace

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

    def test_list_resources(self):
        pause(10)
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
        self.assertIn(self.test_resource_name, result)

    def test_list_resources_with_properties(self):
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

        # Test layer included
        test_resource_in = False

        for r in result:
            if r['name'] == self.test_resource_name:
                test_resource_in = True
                break

        self.assertTrue(test_resource_in)

    def test_list_layers(self):
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
        self.assertIn(self.test_layer_name, result)

    def test_list_layers_with_properties(self):
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

        # Test layer included
        test_layer_in = False

        for r in result:
            if r['name'] == self.test_layer_name:
                test_layer_in = True
                break

        self.assertTrue(test_layer_in)

    def test_list_layer_groups(self):
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
        self.assertIn(self.test_layer_group_name, result)

    def test_list_layer_groups_with_properties(self):
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

        # Test layer included
        test_layer_group_in = False

        for r in result:
            if r['name'] == self.test_layer_group_name:
                test_layer_group_in = True
                break

        self.assertTrue(test_layer_group_in)

    def test_get_resource(self):
        # Execute
        response = self.engine.get_resource(resource_id=self.test_resource_name, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response['result']

        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('workspace', result)
        self.assertEqual(result['workspace'], self.test_resource_workspace)

    def test_get_resource_with_workspace(self):
        # Execute
        response = self.engine.get_resource(resource_id=self.test_resource_identifier,
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
        self.assertIn('workspace', result)
        self.assertEqual(result['name'], self.test_resource_name)
        self.assertEqual(result['workspace'], self.test_resource_workspace)

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
        pass

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

    def test_gets_layer_group(self):
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
        pass

    def test_update_layer_group(self):
        pass

    def test_delete_resource(self):
        pass

    def test_delete_resource_belongs_to_layer(self):
        pass

    def test_delete_resource_recurse(self):
        pass

    def test_delete_resource_does_not_exist(self):
        pass

    def test_delete_layer(self):
        pass

    def test_delete_layer_belongs_to_group(self):
        pass

    def test_delete_layer_recurse(self):
        pass

    def test_delete_layer_does_not_exist(self):
        pass

    def test_delete_layer_group(self):
        pass

    def test_delete_layer_group_does_not_exist(self):
        pass

    def test_create_layer_group(self):
        pass

    def test_create_layer_group_mismatch_layers_styles(self):
        pass

    def test_create_shapefile_resource(self):
        pass

    def test_create_coverage_resource(self):
        pass