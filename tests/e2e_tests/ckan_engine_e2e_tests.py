import os
import random
import string
import unittest
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


class TestCkanDatasetEngine(unittest.TestCase):

    def setUp(self):
        # Auth
        self.endpoint = TEST_CKAN_DATASET_SERVICE["ENDPOINT"]
        self.apikey = TEST_CKAN_DATASET_SERVICE["APIKEY"]
        self.username = TEST_CKAN_DATASET_SERVICE["USERNAME"]

        # Files
        self.tests_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.files_root = os.path.join(self.tests_root, "files")
        self.support_root = os.path.join(self.tests_root, "support")

        # Create Test Engine
        self.engine = CkanDatasetEngine(endpoint=self.endpoint, apikey=self.apikey)

        # Create Test Organization
        self.test_org = random_string_generator(10)
        data_dict = {"name": self.test_org, "users": [{"name": self.username}]}
        url, data, headers = self.engine._prepare_request(
            "organization_create", data_dict=data_dict, apikey=self.apikey
        )
        status_code, response_text = self.engine._execute_request(url, data, headers)
        if status_code != 200:
            raise requests.RequestException(
                "Unable to create group: {}".format(response_text)
            )

        # Create Test Dataset
        self.test_dataset_name = random_string_generator(10)
        dataset_result = self.engine.create_dataset(
            name=self.test_dataset_name, version="1.0", owner_org=self.test_org
        )
        if not dataset_result["success"]:
            raise requests.RequestException(
                "Unable to create test dataset: {}".format(dataset_result["error"])
            )
        self.test_dataset = dataset_result["result"]

        # Create Test Resource
        self.test_resource_name = random_string_generator(10)
        self.test_resource_url = "http://home.byu.edu"
        resource_result = self.engine.create_resource(
            self.test_dataset_name, url=self.test_resource_url, format="zip"
        )
        if not resource_result["success"]:
            raise requests.RequestException(
                "Unable to create test resource: {}".format(resource_result["error"])
            )
        self.test_resource = resource_result["result"]

    def tearDown(self):
        pass
        # Delete test resource and dataset
        self.engine.delete_dataset(dataset_id=self.test_dataset_name)

    def test_create_dataset(self):
        # Setup
        new_dataset_name = random_string_generator(10)

        # Execute
        result = self.engine.create_dataset(
            name=new_dataset_name, owner_org=self.test_org
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Should return the new one
        self.assertEqual(new_dataset_name, result["result"]["name"])

        # TEST search_datasets
        result = self.engine.search_datasets(
            query={"name": new_dataset_name}, console=False
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Check search results
        search_results = result["result"]["results"]
        self.assertIn(new_dataset_name, search_results[0]["name"])
        self.assertIn(self.test_org, search_results[0]["organization"]["name"])

        # TEST list_datasets
        # Execute
        result = self.engine.list_datasets()

        # Verify Success
        self.assertTrue(result["success"])
        self.assertIn(new_dataset_name, result["result"])

        # Delete
        result = self.engine.delete_dataset(dataset_id=new_dataset_name)

        # Check if success
        self.assertTrue(result["success"])

    def test_create_resource_file(self):
        # Prepare
        file_name = "upload_test.txt"
        save_name = random_string_generator(10)
        file_to_upload = os.path.join(self.support_root, file_name)

        # Execute

        result = self.engine.create_resource(
            dataset_id=self.test_dataset_name, name=save_name, file=file_to_upload
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify name and url_type (which should be upload if file upload)
        self.assertIn(save_name, result["result"]["name"])
        self.assertEqual(result["result"]["url_type"], "upload")

        # TEST search resource
        # Execute
        result = self.engine.search_resources(query={"name": save_name})

        # Verify Success
        self.assertTrue(result["success"])
        self.assertIn(save_name, result["result"]["results"][-1]["name"])

        # Delete
        result = self.engine.delete_resource(
            resource_id=result["result"]["results"][-1]["id"]
        )
        self.assertTrue(result["success"])

    def test_create_resource_url(self):
        # Prepare
        new_resource_name = random_string_generator(10)
        new_resource_url = "http://home.byu.edu/"

        # Execute

        result = self.engine.create_resource(
            dataset_id=self.test_dataset_name,
            url=new_resource_url,
            name=new_resource_name,
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify name and url_type (which should be upload if file upload)
        self.assertIn(new_resource_name, result["result"]["name"])
        self.assertEqual(result["result"]["url"], new_resource_url)

        # TEST search resource
        # Execute
        result = self.engine.search_resources(query={"name": new_resource_name})

        # Verify Success
        self.assertTrue(result["success"])
        self.assertIn(new_resource_name, result["result"]["results"][-1]["name"])
        self.assertIn(new_resource_url, result["result"]["results"][-1]["url"])

        # Delete
        result = self.engine.delete_resource(
            resource_id=result["result"]["results"][-1]["id"]
        )
        self.assertTrue(result["success"])

    def test_update_dataset(self):
        # Setup
        notes = random_string_generator(10)
        author = random_string_generator(5)

        # Execute
        result = self.engine.update_dataset(
            dataset_id=self.test_dataset_name, author=author, notes=notes
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify new property
        self.assertEqual(result["result"]["author"], author)
        self.assertEqual(result["result"]["notes"], notes)

        # TEST get_dataset
        # Execute
        result = self.engine.get_dataset(dataset_id=self.test_dataset_name)

        # Verify Success
        self.assertTrue(result["success"])

        # Verify Name
        self.assertEqual(result["result"]["name"], self.test_dataset_name)
        self.assertEqual(result["result"]["author"], author)
        self.assertEqual(result["result"]["notes"], notes)

        # TEST download_dataset
        location = self.files_root

        result = self.engine.download_dataset(self.test_dataset_name, location=location)

        # Result will return list of the file with .zip at the end. Check here
        self.assertIn(".zip", result[0][-4:].lower())

        download_file = os.path.basename(result[0])

        location_final = os.path.join(self.files_root, download_file)

        # Delete the file
        if os.path.isfile(location_final):
            os.remove(location_final)
        else:
            raise AssertionError("No file has been downloaded")

        # TEST delete_dataset
        # Execute
        result = self.engine.delete_dataset(dataset_id=self.test_dataset_name)

        # Confirm Success
        self.assertTrue(result["success"])

        # Delete requests should return nothing
        self.assertEqual(result["result"], None)

    def test_update_resource(self):
        # Get Resource ID
        result = self.engine.get_dataset(dataset_id=self.test_dataset_name)

        resource_id = result["result"]["resources"][0]["id"]

        # Setup
        file_name = "upload_test.txt"
        file_to_upload = os.path.join(self.support_root, file_name)
        description_new = random_string_generator(10)

        # Execute
        result = self.engine.update_resource(
            resource_id=resource_id, file=file_to_upload, description=description_new
        )

        # Verify Success
        self.assertTrue(result["success"])

        # Verify Name (should be the same as the file uploaded by default)
        self.assertEqual(result["result"]["name"], file_name)
        self.assertEqual(result["result"]["description"], description_new)

        # TEST get_resource
        # Execute
        result = self.engine.get_resource(resource_id=resource_id)

        # Verify Success
        self.assertTrue(result["success"])

        # Verify Properties
        self.assertEqual(result["result"]["name"], file_name)
        self.assertEqual(result["result"]["description"], description_new)

        # TEST download_resource
        location = self.files_root

        result = self.engine.download_resource(
            resource_id=resource_id, location=location
        )

        # Result will return list of the file with .zip at the end. Check here
        self.assertIn(".zip", result[-4:].lower())
        download_file = os.path.basename(result)

        location_final = os.path.join(self.files_root, download_file)

        # Delete the file
        if os.path.isfile(location_final):
            os.remove(location_final)
        else:
            raise AssertionError("No file has been downloaded")

        # TEST delete_resource
        # Execute
        result = self.engine.delete_resource(resource_id=resource_id)

        # Verify Success
        self.assertTrue(result["success"])

        # Delete requests should return nothing
        self.assertEqual(result["result"], None)

    def test_validate(self):
        self.engine.validate()

    def test_validate_status_code(self):
        self.engine2 = CkanDatasetEngine(
            endpoint="http://localhost:5000/api/a/action/",
            apikey=TEST_CKAN_DATASET_SERVICE["APIKEY"],
        )
        self.assertRaises(AssertionError, self.engine2.validate)
