from io import StringIO
import json
import os
import random
import string
import unittest
from unittest import mock

import requests

from tethys_dataset_services.engines import CkanDatasetEngine


try:
    from ..test_config import TEST_CKAN_DATASET_SERVICE

except ImportError:
    print(
        'ERROR: To perform tests, you must create a file in the "tests" package called "test_config.py". In this file'
        'provide a dictionary called "TEST_CKAN_DATASET_SERVICE" with keys "API_ENDPOINT" and "APIKEY".'
    )
    exit(1)


def random_string_generator(size):
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(size))


class MockJsonResponse(object):
    def __init__(self, status_code, success=True, result=None, json_format=True):
        self.status_code = status_code
        data = dict()
        data["success"] = success
        if not success:
            data["error"] = {"message": "failed message"}
        data["result"] = result
        # data['get'] = get_data
        if json_format:
            self.text = json.dumps(data)
        else:
            self.text = "Not a JSON object"
            # self.encode = encode


class MockResponse(object):
    def __init__(self, status_code, text=None, json=None, reason=None):
        self.status_code = status_code
        self.text = text
        self.json_obj = json
        self.reason = reason

    def json(self):
        return self.json_obj


class TestCkanDatasetEngine(unittest.TestCase):

    def setUp(self):
        # Create Test Engine
        self.engine = CkanDatasetEngine(
            endpoint=TEST_CKAN_DATASET_SERVICE["ENDPOINT"],
            apikey=TEST_CKAN_DATASET_SERVICE["APIKEY"],
        )

        # Test Dataset Name
        self.test_dataset_name = random_string_generator(10)

        # Test Resource Variables
        self.test_resource_name = random_string_generator(10)
        self.test_resource_url = "http://home.byu.edu"

        # File paths
        self.tests_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.files_path = os.path.join(self.tests_path, "files")
        self.support_path = os.path.join(self.tests_path, "support")

    def tearDown(self):
        pass

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_list_datasets_defaults(self, mock_post):
        mock_post.return_value = MockJsonResponse(200, result="Datasetname")

        # Execute
        result = self.engine.list_datasets()

        # Verify Success
        self.assertTrue(result["success"])
        self.assertIn("Datasetname", result["result"])

    @mock.patch("tethys_dataset_services.engines.ckan_engine.log")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_list_datasets_defaults_no_json(self, mock_post, mock_log):
        mock_post.return_value = MockJsonResponse(
            201, result="Datasetname", json_format=False
        )

        # Execute
        self.engine.list_datasets()

        mock_log.exception.assert_called()
        call_args = mock_log.exception.call_args_list
        self.assertIn("Status Code 201", call_args[0][0][0])

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_list_datasets_with_resources(self, mock_post):
        mock_post.return_value = MockJsonResponse(200, result="Datasetname")
        # Execute
        result = self.engine.list_datasets(with_resources=True)

        # Verify Success
        self.assertTrue(result["success"])
        self.assertIn("Datasetname", result["result"])

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_list_datasets_with_params(self, mock_post):
        data_list = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
        mock_post.return_value = MockJsonResponse(200, result=data_list)
        # Setup
        limit = 10
        number_all = len(self.engine.list_datasets()["result"])

        # Execute twice with offsets different
        data_list = ["2", "3", "4", "5", "6", "7", "8", "9", "10"]
        mock_post.return_value = MockJsonResponse(200, result=data_list)
        result_page_1 = self.engine.list_datasets(limit=limit, offset=1, console=False)

        data_list = ["3", "4", "5", "6", "7", "8", "9", "10"]
        mock_post.return_value = MockJsonResponse(200, result=data_list)
        result_page_2 = self.engine.list_datasets(limit=limit, offset=2)

        # Verify success
        self.assertTrue(result_page_1["success"])
        self.assertTrue(result_page_2["success"])

        # Count the results
        page_1_count = len(result_page_1["result"])
        page_2_count = len(result_page_2["result"])

        # Verify count (should be less than or equal to limit)
        self.assertLessEqual(page_1_count, limit)
        self.assertLessEqual(page_2_count, limit)

        # If there are more than 5 datasets, the results should be different
        if number_all > 5:
            self.assertNotEqual(result_page_1, result_page_2)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_search_resources(self, mock_post):
        result_data = {"results": [{"format": "ZIP"}, {"format": "ZIP"}]}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.search_resources(
            query={"format": "zip", "contents": "html"}
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Check search results if they exist
        search_results = result["result"]["results"]

        if len(search_results) > 1:
            for result in search_results:
                self.assertIn("zip", result["format"].lower())

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_search_datasets(self, mock_post):
        version = "1.0"
        result_data = {"results": [{"version": version}, {"version": version}]}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute

        result = self.engine.search_datasets(query={"version": version}, console=False)

        # Verify Success
        self.assertTrue(result["success"])

        # Check search results if they exist
        search_results = result["result"]["results"]

        if len(search_results) > 1:
            for result in search_results:
                self.assertIn("version", result)
                self.assertEqual(result["version"], version)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_search_datasets_filtered(self, mock_post):
        version = "1.0"
        result_data = {"results": [{"version": version}, {"version": version}]}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute

        result = self.engine.search_datasets(
            filtered_query={"version": version}, console=False
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Check search results if they exist
        search_results = result["result"]["results"]

        if len(search_results) > 1:
            for result in search_results:
                self.assertIn("version", result)
                self.assertEqual(result["version"], version)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_search_datasets_no_queries(self, mock_post):
        version = "1.0"
        result_data = {"results": [{"version": version}, {"version": version}]}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        self.assertRaises(Exception, self.engine.search_datasets, console=False)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_create_dataset(self, mock_post):
        # Setup
        new_dataset_name = random_string_generator(10)
        result_data = {"name": new_dataset_name}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.create_dataset(name=new_dataset_name)

        # Verify Success
        self.assertTrue(result["success"])

        # Should return the new one
        self.assertEqual(new_dataset_name, result["result"]["name"])

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_create_resource_url(self, mock_post):
        # Setup
        new_resource_name = random_string_generator(5)
        new_resource_url = "http://home.byu.edu"
        result_data = {
            "name": new_resource_name,
            "url": new_resource_url,
            "id": self.test_dataset_name,
        }
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.create_resource(
            dataset_id=self.test_dataset_name,
            url=new_resource_url,
            name=new_resource_name,
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify name and url
        self.assertEqual(new_resource_name, result["result"]["name"])
        self.assertEqual(new_resource_url, result["result"]["url"])

    def test_create_resource_url_file(self):
        file_name = "upload_test.txt"
        file_to_upload = os.path.join(self.support_path, file_name)

        # Setup
        new_resource_url = "http://home.byu.edu"

        # Execute file=file_to_upload
        self.assertRaises(
            IOError,
            self.engine.create_resource,
            dataset_id=self.test_dataset_name,
            url=new_resource_url,
            file=file_to_upload,
        )

    def test_create_resource_no_url_file(self):
        # Execute
        self.assertRaises(
            IOError, self.engine.create_resource, dataset_id=self.test_dataset_name
        )

    def test_create_resource_file_not_exist(self):
        file_name = "upload_test1.txt"
        file_to_upload = os.path.join(self.support_path, file_name)

        # Execute file=file_to_upload
        self.assertRaises(
            IOError,
            self.engine.create_resource,
            dataset_id=self.test_dataset_name,
            file=file_to_upload,
        )

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_create_resource_file_upload(self, mock_post):
        # Prepare
        file_name = "upload_test.txt"
        file_to_upload = os.path.join(self.support_path, file_name)
        result_data = {
            "name": file_name,
            "url_type": "upload",
            "id": self.test_dataset_name,
        }
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.create_resource(
            dataset_id=self.test_dataset_name, file=file_to_upload, console=False
        )

        # Verify Success
        self.assertTrue(result["success"], result)

        # Verify name and url_type (which should be upload if file upload)
        self.assertEqual(result["result"]["name"], "upload_test.txt")
        self.assertEqual(result["result"]["url_type"], "upload")

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_create_resource_file_upload_no_ext(self, mock_post):
        # Prepare
        file_name = "upload_test.txt"
        upload_file_name = "testfile"
        file_to_upload = os.path.join(self.support_path, file_name)
        result_data = {
            "name": upload_file_name,
            "url_type": "upload",
            "id": self.test_dataset_name,
        }
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.create_resource(
            dataset_id=self.test_dataset_name,
            file=file_to_upload,
            name=upload_file_name,
            console=False,
        )

        # Verify Success
        self.assertTrue(result["success"], result)

        # Verify name and url_type (which should be upload if file upload)
        self.assertEqual(upload_file_name, result["result"]["name"])
        self.assertEqual(result["result"]["url_type"], "upload")

    @mock.patch("tethys_dataset_services.engines.ckan_engine.pprint")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_get_dataset(self, mock_post, _):
        result_data = {"name": self.test_dataset_name, "id": self.test_dataset_name}
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.get_dataset(
            dataset_id=self.test_dataset_name, console=True
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify Name
        self.assertEqual(result["result"]["name"], self.test_dataset_name)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.pprint")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_get_resource(self, mock_post, mock_pprint):
        result_data = {"name": self.test_dataset_name, "url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.get_resource(
            resource_id=self.test_resource_name, console=True
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify Properties
        self.assertEqual(result["result"]["url"], self.test_resource_url)

        mock_pprint.pprint.assert_called()

    @mock.patch("tethys_dataset_services.engines.ckan_engine.log")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.pprint")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_get_resource_console_error(self, mock_post, mock_pprint, mock_log):
        mock_pprint.pprint.side_effect = Exception("Fake Exception")

        result_data = {"name": self.test_dataset_name, "url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.get_resource(
            resource_id=self.test_resource_name, console=True
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify Properties
        self.assertEqual(result["result"]["url"], self.test_resource_url)

        mock_pprint.pprint.assert_called()
        mock_log.exception.assert_called()

    @mock.patch("tethys_dataset_services.engines.ckan_engine.log")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_get_resource_get_error(self, mock_post, mock_log):
        result_data = {"name": self.test_dataset_name, "url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(
            200, result=result_data, success=False
        )

        # Execute
        result = self.engine.get_resource(
            resource_id=self.test_resource_name, console=True
        )

        # Verify Success
        self.assertFalse(result["success"])

        # Verify Properties
        self.assertIn("failed message", result["error"]["message"])
        self.assertEqual(result["result"]["url"], self.test_resource_url)
        mock_log.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_update_dataset(self, mock_post):
        # Setup
        test_version = "2.0"
        result_data = {
            "version": test_version,
            "resources": self.test_resource_name,
            "tags": "tag_test",
        }
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.update_dataset(
            dataset_id=self.test_dataset_name, version=test_version
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify new version property
        self.assertEqual(result["result"]["version"], test_version)
        self.assertEqual(
            result["result"]["resources"],
            self.test_resource_name,
        )
        self.assertEqual(result["result"]["tags"], "tag_test")

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_update_resource_property_change(self, mock_post):
        # Setup
        new_format = "web"
        result_data = {"format": new_format, "url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.update_resource(
            resource_id=self.test_resource_name, format=new_format
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify new format
        self.assertEqual(result["result"]["format"], new_format)
        self.assertEqual(result["result"]["url"], self.test_resource_url)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_update_resource_url_change(self, mock_post):
        # Setup
        new_url = "http://www.utah.edu"
        result_data = {"url": new_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.update_resource(
            resource_id=self.test_resource_name, url=new_url
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify New URL Property
        self.assertEqual(result["result"]["url"], new_url)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_update_resource_file_upload(self, mock_post):
        # Setup
        file_name = "upload_test.txt"
        file_to_upload = os.path.join(self.support_path, file_name)

        result_data = {
            "name": file_name,
            "id": self.test_dataset_name,
            "url": self.test_resource_url,
        }

        mock_post.return_value = MockJsonResponse(200, result=result_data)

        # Execute
        result = self.engine.update_resource(
            resource_id=self.test_resource_name, file=file_to_upload, console=False
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify Name (should be the same as the file uploaded by default)
        self.assertEqual(result["result"]["name"], file_name)
        self.assertEqual(result["result"]["url"], self.test_resource_url)

        # URL should be different than original when file upload executes
        # self.assertNotEqual(result['result']['url'], self.test_resource['url'])

    def test_update_resource_url_file(self):
        file_name = "upload_test.txt"
        file_to_upload = os.path.join(self.support_path, file_name)

        # Setup
        new_resource_url = "http://home.byu.edu"

        # Execute file=file_to_upload
        self.assertRaises(
            IOError,
            self.engine.update_resource,
            resource_id=self.test_resource_name,
            url=new_resource_url,
            file=file_to_upload,
        )

    def test_update_resource_file_not_exist(self):
        file_name = "upload_test1.txt"
        file_to_upload = os.path.join(self.support_path, file_name)

        # Execute file=file_to_upload
        self.assertRaises(
            IOError,
            self.engine.update_resource,
            resource_id=self.test_resource_name,
            file=file_to_upload,
        )

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_delete_resource(self, mock_post):
        result_data = None
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.delete_resource(resource_id=self.test_resource_name)

        # Verify Success
        self.assertTrue(result["success"])

        # Delete requests should return nothing
        self.assertEqual(result["result"], None)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_delete_dataset(self, mock_post):
        result_data = None
        mock_post.return_value = MockJsonResponse(200, result=result_data)
        # Execute
        result = self.engine.delete_dataset(
            dataset_id=self.test_dataset_name, console=False
        )

        # Confirm Success
        self.assertTrue(result["success"])

        # Delete requests should return nothing
        self.assertEqual(result["result"], None)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_download_resource(self, mock_post):
        location = self.files_path
        local_file_name = "test_resource.test"
        location_final = os.path.join(self.files_path, local_file_name)

        result_data = {"url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        result = self.engine.download_resource(
            self.test_resource_name, location=location, local_file_name=local_file_name
        )

        # Result will return the local file path. Check here
        self.assertEqual(location_final, result)

        # Delete the file
        if os.path.isfile(location_final):
            os.remove(location_final)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_download_resource_no_location(self, mock_post):
        local_file_name = "test_resource.test"
        location_check = os.path.join("./", local_file_name)

        result_data = {"url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        result = self.engine.download_resource(
            self.test_resource_name, local_file_name=local_file_name
        )

        # Result will return the local file path. Check here
        self.assertEqual(location_check, result)

        # Delete the file
        if os.path.isfile(location_check):
            os.remove(location_check)

    @mock.patch(
        "tethys_dataset_services.engines.ckan_engine.CkanDatasetEngine.get_resource"
    )
    def test_download_resource_not_exist(self, mock_ckan):
        mock_ckan.return_value = {"success": False}
        self.assertRaises(
            Exception, self.engine.download_resource, self.test_dataset_name
        )

        mock_ckan.assert_called_with(self.test_dataset_name, console=False)

    @mock.patch("sys.stdout", new_callable=StringIO)
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.get")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_download_resource_request_get_exception(
        self, mock_post, mock_get, mock_print
    ):
        mock_get.side_effect = Exception("Requests.get Exception")
        location = self.files_path
        local_file_name = "test_resource.test"

        result_data = {"url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        self.engine.download_resource(
            self.test_resource_name, location=location, local_file_name=local_file_name
        )

        output = mock_print.getvalue()

        # check results
        self.assertIn("Requests.get Exception", output)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.warnings")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_download_resouce(self, mock_post, mock_warnings):
        location = self.files_path
        local_file_name = "test_resource.test"
        location_final = os.path.join(self.files_path, local_file_name)

        result_data = {"url": self.test_resource_url}
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        result = self.engine.download_resouce(
            self.test_resource_name, location=location, local_file_name=local_file_name
        )

        # Result will return None instead of the local path file.
        self.assertEqual(None, result)

        # Check if file is created and delete it
        if os.path.isfile(location_final):
            os.remove(location_final)
        else:
            raise AssertionError("Resource has not been downloaded")

        # Delete the file
        if os.path.isfile(location_final):
            os.remove(location_final)

        mock_warnings.warn.assert_called()

    @mock.patch("tethys_dataset_services.engines.ckan_engine.pprint")
    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.post")
    def test_download_dataset(self, mock_post, _):
        location = self.files_path
        location_final = os.path.join(self.files_path, "resource1.txt")
        result_check = [location_final]

        result_data = {
            "resources": [
                {
                    "name": "resource1",
                    "id": "resource2",
                    "format": "txt",
                    "url": self.test_resource_url,
                }
            ]
        }
        mock_post.return_value = MockJsonResponse(200, result=result_data)

        result = self.engine.download_dataset(
            self.test_dataset_name, location=location, console=True
        )

        # Result will return list of the local file path. Check here
        self.assertEqual(result_check, result)

        # Delete the file
        if os.path.isfile(location_final):
            os.remove(location_final)

    @mock.patch(
        "tethys_dataset_services.engines.ckan_engine.CkanDatasetEngine.get_dataset"
    )
    def test_download_dataset_not_exist(self, mock_ckan):
        mock_ckan.return_value = {"success": False}
        self.assertRaises(
            Exception, self.engine.download_dataset, self.test_dataset_name
        )

        mock_ckan.assert_called_with(self.test_dataset_name, console=False)

    def test_type(self):
        response = self.engine.type
        expected_response = "CKAN"

        # Check Response
        self.assertEqual(response, expected_response)

    def test_prepare_request(self):
        method = "resource_show"
        result = self.engine._prepare_request(
            method, apikey=TEST_CKAN_DATASET_SERVICE["APIKEY"]
        )

        # Check Result, result[0] is url, result[1] is data_dict, result[2] is headers
        self.assertIn(TEST_CKAN_DATASET_SERVICE["APIKEY"], result[2]["X-CKAN-API-Key"])
        self.assertIn(method, result[0])

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.get")
    def test_validate(self, mock_get):
        mock_get.side_effect = requests.exceptions.MissingSchema
        self.assertRaises(AssertionError, self.engine.validate)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.get")
    def test_validate_status_code(self, mock_get):
        self.engine = CkanDatasetEngine(
            endpoint="http://localhost:5000/api/3/action",
            apikey=TEST_CKAN_DATASET_SERVICE["APIKEY"],
        )

        result_data = {"resources": self.test_resource_name, "version": "1.0"}
        mock_get.return_value = MockJsonResponse(201, result=result_data)

        self.assertRaises(AssertionError, self.engine.validate)

    @mock.patch("tethys_dataset_services.engines.ckan_engine.requests.get")
    def test_validate_no_version(self, mock_get):
        mock_get.return_value = MockResponse(200, json="")

        self.assertRaises(AssertionError, self.engine.validate)
