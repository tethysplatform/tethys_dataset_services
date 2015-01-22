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


class TestGeoServerDatasetEngine(unittest.TestCase):

    def setUp(self):
        # Globals
        self.debug = False

        # Create Test Engine
        self.engine = GeoServerSpatialDatasetEngine(endpoint=TEST_GEOSERVER_DATASET_SERVICE['ENDPOINT'],
                                                    username=TEST_GEOSERVER_DATASET_SERVICE['USERNAME'],
                                                    password=TEST_GEOSERVER_DATASET_SERVICE['PASSWORD'])

        # Test layer
        self.test_resource_name = 'roads'
        self.test_resource_workspace = 'sf'
        self.test_resource_store = 'sf'
        self.test_layer_name = 'streams'
        self.test_layer_group_name = 'tasmania'

        # Create Test Dataset
        # self.test_dataset_name = random_string_generator(10)
        # dataset_result = self.engine.create_dataset(name=self.test_dataset_name, version='1.0')
        # self.test_dataset = dataset_result['result']
        #
        # # Create Test Resource
        # self.test_resource_name = random_string_generator(10)
        # self.test_resource_url = 'http://home.byu.edu'
        # resource_result = self.engine.create_resource(self.test_dataset_name, url=self.test_resource_url, format='zip')
        # self.test_resource = resource_result['result']

    def tearDown(self):
        # Delete test resource and dataset
        # self.engine.delete_resource(resource_id=self.test_resource['id'])
        # self.engine.delete_dataset(dataset_id=self.test_dataset_name)
        pass

    def test_list_resources(self):
        # Execute
        result = self.engine.list_resources(debug=self.debug)

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        self.assertIn(self.test_resource_name, result)

    def test_list_resources_with_properties(self):
        # Execute
        result = self.engine.list_resources(with_properties=True, debug=self.debug)

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
        result = self.engine.list_layers(debug=self.debug)

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        self.assertIn(self.test_layer_name, result)

    def test_list_layers_with_properties(self):
        # Execute
        result = self.engine.list_layers(with_properties=True, debug=self.debug)

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
        result = self.engine.list_layer_groups(debug=self.debug)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer group listed
        self.assertIn(self.test_layer_group_name, result)

    def test_list_layer_groups_with_properties(self):
        # Execute
        result = self.engine.list_layer_groups(with_properties=True, debug=self.debug)

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
        result = self.engine.get_resource(resource_id=self.test_resource_name,
                                          debug=self.debug)
        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('workspace', result)
        self.assertEqual(result['workspace'], self.test_resource_workspace)

    def test_get_resource_with_workspace(self):
        # Execute
        result = self.engine.get_resource(resource_id=self.test_resource_name,
                                          workspace=self.test_resource_workspace,
                                          debug=self.debug)
        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('name', result)
        self.assertIn('workspace', result)
        self.assertEqual(result['name'], self.test_resource_name)
        self.assertEqual(result['workspace'], self.test_resource_workspace)

    def test_get_resource_with_store(self):
        # Execute
        result = self.engine.get_resource(resource_id=self.test_resource_name,
                                          store=self.test_resource_workspace,
                                          debug=self.debug)
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
        result = self.engine.get_layer(layer_id=self.test_layer_name, debug=self.debug)

        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('name', result)
        self.assertEqual(result['name'], self.test_layer_name)

    def test_gets_layer_group(self):
        # Execute
        result = self.engine.get_layer_group(layer_group_id=self.test_layer_group_name, debug=self.debug)

        # Type
        self.assertIsInstance(result, dict)

        # Properties
        self.assertIn('name', result)
        self.assertEqual(result['name'], self.test_layer_group_name)

    def test_update_resource(self):
        # Setup
        new_title = random_string_generator(15)

        # Execute
        result = self.engine.update_resource(resource_id=self.test_resource_name,
                                             title=new_title,
                                             debug=self.debug)

        # Get new resource
        updated_result = self.engine.get_resource(resource_id=self.test_resource_name, debug=self.debug)

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(updated_result['title'], new_title)
        self.assertEqual(result['title'], updated_result['title'])
        self.assertEqual(result, updated_result)

    def test_update_resource_workspace(self):
        # Setup
        new_title = random_string_generator(15)

        # Execute
        result = self.engine.update_resource(resource_id=self.test_resource_name,
                                             workspace=self.test_resource_workspace,
                                             title=new_title,
                                             debug=self.debug)

        # Get new resource
        updated_result = self.engine.get_resource(resource_id=self.test_resource_name,
                                                  workspace=self.test_resource_workspace,
                                                  debug=self.debug)

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(updated_result['title'], new_title)
        self.assertEqual(result['title'], updated_result['title'])
        self.assertEqual(result, updated_result)

    def test_update_resource_store(self):
        # Setup
        new_title = random_string_generator(15)

        # Execute
        result = self.engine.update_resource(resource_id=self.test_resource_name,
                                             store=self.test_resource_store,
                                             title=new_title,
                                             debug=self.debug)

        # Get new resource
        updated_result = self.engine.get_resource(resource_id=self.test_resource_name,
                                                  store=self.test_resource_store,
                                                  debug=self.debug)

        # Properties
        self.assertEqual(result['title'], new_title)
        self.assertEqual(updated_result['title'], new_title)
        self.assertEqual(result['title'], updated_result['title'])
        self.assertEqual(result, updated_result)