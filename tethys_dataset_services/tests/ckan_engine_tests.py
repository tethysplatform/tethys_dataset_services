import os
import random
import string
import unittest
import json
import mock

from tethys_dataset_services.engines import CkanDatasetEngine

try:
    from tethys_dataset_services.tests.test_config import TEST_CKAN_DATASET_SERVICE

except ImportError:
    print('ERROR: To perform tests, you must create a file in the "tests" package called "test_config.py". In this file'
          'provide a dictionary called "TEST_CKAN_DATASET_SERVICE" with keys "API_ENDPOINT" and "APIKEY".')
    exit(1)


def random_string_generator(size):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


class MockJsonResponse(object):
    def __init__(self, status_code, success=True, result=None, encoding='utf-8'):
        self.status_code = status_code
        data = dict()
        data['success'] = success
        data['result'] = result
        data['encoding'] = encoding
        # data['get'] = get_data
        self.text = json.dumps(data)


class TestCkanDatasetEngine(unittest.TestCase):

    def setUp(self):
        # Create Test Engine
        # self.engine = CkanDatasetEngine(endpoint=TEST_CKAN_DATASET_SERVICE['ENDPOINT'],
        #                                 apikey=TEST_CKAN_DATASET_SERVICE['APIKEY'],
        #                                 username=TEST_CKAN_DATASET_SERVICE['USERNAME'],
        #                                 password=TEST_CKAN_DATASET_SERVICE['PASSWORD'])

        self.engine = CkanDatasetEngine(endpoint=TEST_CKAN_DATASET_SERVICE['ENDPOINT'],
                                        apikey=TEST_CKAN_DATASET_SERVICE['APIKEY'])

        # Create Test Dataset
        self.test_dataset_name = random_string_generator(10)
        # dataset_result = self.engine.create_dataset(name=self.test_dataset_name, version='1.0', owner_org='aquaveo')
        # self.test_dataset = dataset_result['result']
        #
        # # Create Test Resource
        self.test_resource_name = random_string_generator(10)
        self.test_resource_url = 'http://home.byu.edu'
        # resource_result = self.engine.create_resource(self.test_dataset_name,
        #                                               url=self.test_resource_url, format='zip')
        # self.test_resource = resource_result['result']

    def tearDown(self):
        pass
        # Delete test resource and dataset
        # self.engine.delete_resource(resource_id=self.test_resource['id'])
        # self.engine.delete_dataset(dataset_id=self.test_dataset_name)

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    # @mock.patch('tethys_dataset_services.engines.ckan_engine.CkanDatasetEngine')
    def test_list_datasets_defaults(self, mock_post):
        mock_post.return_value = MockJsonResponse(200, result='Datasetname')

        # Execute
        result = self.engine.list_datasets()

        # Verify Success
        self.assertTrue(result['success'])
        self.assertIn('Datasetname', result['result'])

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_list_datasets_with_resources(self, mock_post):
        mock_post.return_value = MockJsonResponse(200, result='Datasetname')
        # Execute
        result = self.engine.list_datasets(with_resources=True)

        # Verify Success
        self.assertTrue(result['success'])
        self.assertIn('Datasetname', result['result'])

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_list_datasets_with_params(self, mock_post):
        data_list=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        mock_post.return_value = MockJsonResponse(200, result=data_list)
        # Setup
        limit = 10
        number_all = len(self.engine.list_datasets()['result'])

        # Execute twice with offsets different
        data_list=['2', '3', '4', '5', '6', '7', '8', '9', '10']
        mock_post.return_value = MockJsonResponse(200, result=data_list)
        result_page_1 = self.engine.list_datasets(limit=limit, offset=1, console=False)

        data_list=['3', '4', '5', '6', '7', '8', '9', '10']
        mock_post.return_value = MockJsonResponse(200, result=data_list)
        result_page_2 = self.engine.list_datasets(limit=limit, offset=2)

        # Verify success
        self.assertTrue(result_page_1['success'])
        self.assertTrue(result_page_2['success'])

        # Count the results
        page_1_count = len(result_page_1['result'])
        page_2_count = len(result_page_2['result'])

        # Verify count (should be less than or equal to limit)
        self.assertLessEqual(page_1_count, limit)
        self.assertLessEqual(page_2_count, limit)

        # If there are more than 5 datasets, the results should be different
        if number_all > 5:
            self.assertNotEqual(result_page_1, result_page_2)

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_search_resources(self, mock_post):
        result_data = {'results': [{'format': 'ZIP'}, {'format': 'ZIP'}]}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.search_resources(query={'format': 'zip'})

        # Verify Success
        self.assertTrue(result['success'])

        # Check search results if they exist
        search_results = result['result']['results']

        if len(search_results) > 1:
            for result in search_results:
                self.assertIn('zip', result['format'].lower())

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_search_datasets(self, mock_post):
        version = '1.0'
        result_data = {'results': [{'version': version}, {'version': version}]}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute

        result = self.engine.search_datasets(query={'version': version}, console=False)

        # Verify Success
        self.assertTrue(result['success'])

        # Check search results if they exist
        search_results = result['result']['results']

        if len(search_results) > 1:
            for result in search_results:
                self.assertIn('version', result)
                self.assertEqual(result['version'], version)

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_create_dataset(self, mock_post):
        # Setup
        new_dataset_name = random_string_generator(10)
        result_data = {'name': new_dataset_name}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.create_dataset(name=new_dataset_name)

        # Verify Success
        self.assertTrue(result['success'])

        # Should return the new one
        self.assertEqual(new_dataset_name, result['result']['name'])

        # Delete
        self.engine.delete_dataset(dataset_id=new_dataset_name)

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_create_resource_url(self, mock_post):
        # Setup
        new_resource_name = random_string_generator(5)
        new_resource_url = 'http://home.byu.edu'
        result_data = {'name': new_resource_name, 'url': new_resource_url,
                       'id': self.test_dataset_name}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.create_resource(dataset_id=self.test_dataset_name,
                                             url=new_resource_url,
                                             name=new_resource_name)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify name and url
        self.assertEqual(new_resource_name, result['result']['name'])
        self.assertEqual(new_resource_url, result['result']['url'])

        # Delete
        self.engine.delete_resource(resource_id=result['result']['id'])

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_create_resource_file_upload(self, mock_post):
        # Prepare
        file_name = 'upload_test.txt'
        file_to_upload = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'support', file_name)
        result_data = {'name': file_name, 'url_type': 'upload',
                       'id': self.test_dataset_name}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.create_resource(dataset_id=self.test_dataset_name, file=file_to_upload, console=False)

        # Verify Success
        self.assertTrue(result['success'], result)

        # Verify name and url_type (which should be upload if file upload)
        self.assertEqual(result['result']['name'], 'upload_test.txt')
        self.assertEqual(result['result']['url_type'], 'upload')

        # Delete
        self.engine.delete_resource(resource_id=result['result']['id'])

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_get_dataset(self, mock_post):
        result_data = {'name': self.test_dataset_name, 'id': self.test_dataset_name}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.get_dataset(dataset_id=self.test_dataset_name, console=True)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify Name
        self.assertEqual(result['result']['name'], self.test_dataset_name)

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_get_resource(self, mock_post):
        result_data = {'name': self.test_dataset_name, 'url': self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.get_resource(resource_id=self.test_resource_name, console=True)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify Properties
        self.assertEqual(result['result']['url'], self.test_resource_url)

    @mock.patch('tethys_dataset_services.engines.ckan_engine.requests.post')
    def test_get_resource_get_error(self, mock_post):
        result_data = {'name': self.test_dataset_name, 'url': self.test_resource_url}
        get_data = {'error': {'message': 'failed message'}}
        mock_post.return_value = MockJsonResponse(200, result=result_data, success=False)

        # Execute
        result = self.engine.get_resource(resource_id=self.test_resource_name, console=True)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify Properties
        self.assertEqual(result['result']['url'], self.test_resource_url)

    def test_update_dataset(self):
        # Setup
        test_version = '2.0'

        # Execute
        result = self.engine.update_dataset(dataset_id=self.test_dataset_name, version=test_version)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify new version property
        self.assertEqual(result['result']['version'], test_version)

    def test_update_resource_property_change(self):
        # Setup
        new_format = 'web'

        # Execute
        result = self.engine.update_resource(resource_id=self.test_resource['id'], format=new_format)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify new format
        self.assertEqual(result['result']['format'], new_format)

    def test_update_resource_url_change(self):
        # Setup
        new_url = 'http://www.utah.edu'

        # Execute
        result = self.engine.update_resource(resource_id=self.test_resource['id'], url=new_url)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify New URL Property
        self.assertEqual(result['result']['url'], new_url)
        self.assertNotEqual(result['result']['url'], self.test_resource['url'])

    def test_update_resource_file_upload(self):
        # Setup
        file_to_upload = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'support', 'upload_test.txt')

        # Execute
        result = self.engine.update_resource(resource_id=self.test_resource['id'], file=file_to_upload, console=False)

        # Verify Success
        self.assertTrue(result['success'])

        # Verify Name (should be the same as the file uploaded by default)
        self.assertEqual(result['result']['name'], 'upload_test.txt')

        # URL should be different than original when file upload executes
        # self.assertNotEqual(result['result']['url'], self.test_resource['url'])

    def test_delete_resource(self):
        # Execute
        result = self.engine.delete_resource(resource_id=self.test_resource['id'])

        # Verify Success
        self.assertTrue(result['success'])

        # Delete requests should return nothing
        self.assertEqual(result['result'], None)

    def test_delete_dataset(self):
        # Execute
        result = self.engine.delete_dataset(dataset_id=self.test_dataset_name, console=False)

        # Confirm Success
        self.assertTrue(result['success'])

        # Delete requests should return nothing
        self.assertEqual(result['result'], None)

    def test_download_resource(self):
        raise NotImplementedError()
        # self.engine.download_resource(resource_id, location='../tests/files/', local_file_name='test_resource.test')

    def test_download_dataset(self):
        raise NotImplementedError()
        # self.engine.download_dataset(dataset_id, console=True)
