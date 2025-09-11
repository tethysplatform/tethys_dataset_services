from io import StringIO
import os
import random
import string
import unittest
from unittest import mock

import geoserver
import requests
from sqlalchemy import create_engine

from tethys_dataset_services.engines import GeoServerSpatialDatasetEngine


def random_string_generator(size):
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(size))


def mock_get_style(name, workspace=None):
    mock_style = mock.NonCallableMagicMock(workspace=workspace)
    mock_style.name = name
    return mock_style


def mock_get_resource(name, **kwargs):
    if "workspace" or "store" in kwargs:
        mock_resource = mock.NonCallableMagicMock()
        mock_resource.name = name
        if "workspace" in kwargs:
            mock_resource.workspace = kwargs["workspace"]
        if "store" in kwargs:
            mock_resource.store = kwargs["store"]
        return mock_resource
    else:
        raise AssertionError(
            "Did not get expected keyword arguments: {}".format(list(kwargs))
        )


def mock_get_resource_create_postgis_feature_resource(name, **kwargs):
    if "workspace" in kwargs:
        raise geoserver.catalog.FailedRequestError()
    elif "store" in kwargs:
        mock_resource = mock.NonCallableMagicMock()
        mock_resource.name = name
        mock_resource.store = kwargs["store"]
        return mock_resource
    else:
        raise AssertionError(
            "Did not get expected keyword arguments: {}".format(list(kwargs))
        )


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
        self.files_root = os.path.join(self.tests_root, "files")

        self.shapefile_name = "test"
        self.shapefile_base = os.path.join(
            self.files_root, "shapefile", self.shapefile_name
        )

        # Create Test Engine
        self.endpoint = "http://fake.geoserver.org:8181/geoserver/rest/"
        self.public_endpoint = "http://fake.public.geoserver.org:8181/geoserver/rest/"
        self.username = "foo"
        self.password = "bar"
        self.auth = (self.username, self.password)

        self.engine = GeoServerSpatialDatasetEngine(
            endpoint=self.endpoint,
            username=self.username,
            password=self.password,
            public_endpoint=self.public_endpoint,
        )

        # Catalog
        self.catalog_endpoint = "http://localhost:8181/geoserver/"
        self.mock_catalog = mock.NonCallableMagicMock(gs_base_url=self.catalog_endpoint)

        # Workspaces
        self.workspace_name = "a-workspace"

        # Store
        self.store_name = "a-store"
        self.mock_store = (
            mock.NonCallableMagicMock()
        )  #: Needs to pass not callable test
        # the "name" attribute needs to be set after create b/c name is a constructor argument
        # http://blog.tunarob.com/2017/04/27/mock-name-attribute/
        self.mock_store.name = self.store_name

        # Default Style
        self.default_style_name = "a-style"
        self.mock_default_style = mock.NonCallableMagicMock(
            workspace=self.workspace_name
        )
        self.mock_default_style.name = self.default_style_name

        # Styles
        self.style_names = ["points", "lines"]
        self.mock_styles = []
        for sn in self.style_names:
            mock_style = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_style.name = sn
            self.mock_styles.append(mock_style)

        # Resources
        self.resource_names = ["foo", "bar", "goo"]
        self.mock_resources = []
        for rn in self.resource_names:
            mock_resource = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_resource.name = rn
            mock_resource.store = self.mock_store
            self.mock_resources.append(mock_resource)

        # Layers
        self.layer_names = ["baz", "bat", "jazz"]
        self.mock_layers = []
        for ln in self.layer_names:
            mock_layer = mock.NonCallableMagicMock(workspace=self.workspace_name)
            mock_layer.name = ln
            mock_layer.store = self.mock_store
            mock_layer.default_style = self.mock_default_style
            mock_layer.styles = self.mock_styles
            self.mock_layers.append(mock_layer)

        # Layer groups
        self.layer_group_names = ["boo", "moo"]
        self.mock_layer_groups = []
        for lgn in self.layer_group_names:
            mock_layer_group = mock.NonCallableMagicMock(
                workspace=self.workspace_name,
                catalog=self.mock_catalog,
                dom="fake-dom",
                layers=self.layer_names,
                style=self.style_names,
            )
            mock_layer_group.name = lgn
            self.mock_layer_groups.append(mock_layer_group)

        # Workspaces
        self.workspace_names = ["b-workspace", "c-workspace"]
        self.mock_workspaces = []
        for wp in self.workspace_names:
            mock_workspace = mock.NonCallableMagicMock()
            mock_workspace.name = wp
            self.mock_workspaces.append(mock_workspace)

        # Stores
        self.store_names = ["b-store", "c-store"]
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
        self.assertIn("success", response_object)

        if isinstance(response_object, dict) and "success" in response_object:
            if response_object["success"] is True:
                self.assertIn("result", response_object)
            elif response_object["success"] is False:
                self.assertIn("error", response_object)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_resources(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.return_value = self.mock_resources

        # Execute
        response = self.engine.list_resources(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        for n in self.resource_names:
            self.assertIn(n, result)

        mc.get_resources.assert_called_with(stores=None, workspaces=None)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_resources_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.return_value = self.mock_resources

        # Execute
        response = self.engine.list_resources(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of dictionaries
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn("name", r)
            self.assertIn(r["name"], self.resource_names)
            self.assertIn("workspace", r)
            self.assertEqual(self.workspace_name, r["workspace"])
            self.assertIn("store", r)
            self.assertEqual(self.store_name, r["store"])

        mc.get_resources.assert_called_with(stores=None, workspaces=None)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_resources_ambiguous_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.side_effect = geoserver.catalog.AmbiguousRequestError()

        # Execute
        response = self.engine.list_resources(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        mc.get_resources.assert_called_with(stores=None, workspaces=None)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_resources_multiple_stores_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resources.side_effect = TypeError()

        # Execute
        response = self.engine.list_resources(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])
        self.assertIn("Multiple stores found named", response["error"])

        mc.get_resources.assert_called_with(stores=None, workspaces=None)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_layers(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layers.return_value = self.mock_layers

        # Execute
        response = self.engine.list_layers(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        for n in self.layer_names:
            self.assertIn(n, result)

        mc.get_layers.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_layers_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layers.return_value = self.mock_layers

        # Execute
        response = self.engine.list_layers(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of dictionaries
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn("name", r)
            self.assertIn(r["name"], self.layer_names)
            self.assertIn("workspace", r)
            self.assertEqual(self.workspace_name, r["workspace"])
            self.assertIn("store", r)
            self.assertEqual(self.store_name, r["store"])
            self.assertIn("default_style", r)
            w_default_style = "{}:{}".format(
                self.workspace_name, self.default_style_name
            )
            self.assertEqual(w_default_style, r["default_style"])
            self.assertIn("styles", r)
            w_styles = [
                "{}:{}".format(self.workspace_name, style) for style in self.style_names
            ]
            for s in r["styles"]:
                self.assertIn(s, w_styles)

        mc.get_layers.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_layer_groups(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.return_value = self.mock_layer_groups

        # Execute
        response = self.engine.list_layer_groups(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer group listed
        for r in result:
            self.assertIn(r, self.layer_group_names)

        mc.get_layergroups.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_layer_groups_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.return_value = self.mock_layer_groups

        # Execute
        response = self.engine.list_layer_groups(with_properties=True, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of dictionaries
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn("name", r)
            self.assertIn(r["name"], self.layer_group_names)
            self.assertIn("workspace", r)
            self.assertEqual(self.workspace_name, r["workspace"])
            self.assertIn("catalog", r)
            self.assertIn("layers", r)
            self.assertEqual(self.layer_names, r["layers"])
            self.assertNotIn("dom", r)

        mc.get_layergroups.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_workspaces(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspaces.return_value = self.mock_workspaces

        # Execute
        response = self.engine.list_workspaces(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer group listed
        for r in result:
            self.assertIn(r, self.workspace_names)

        mc.get_workspaces.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_stores(self, mock_catalog):
        mc = mock_catalog()
        mc.get_stores.return_value = self.mock_stores

        # Execute
        response = self.engine.list_stores(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer group listed
        for r in result:
            self.assertIn(r, self.store_names)

        mc.get_stores.assert_called_with(workspaces=[])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_stores_invalid_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_stores.return_value = self.mock_stores
        mc.get_stores.side_effect = AttributeError()

        workspace = "invalid"

        # Execute
        response = self.engine.list_stores(workspace=workspace, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # False
        self.assertFalse(response["success"])
        self.assertIn("Invalid workspace", response["error"])
        mc.get_stores.assert_called_with(workspaces=[workspace])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_styles(self, mock_catalog):
        mc = mock_catalog()
        mc.get_styles.return_value = self.mock_styles

        # Execute
        response = self.engine.list_styles(debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        for n in self.style_names:
            self.assertIn(n, result)

        mc.get_styles.assert_called_with(workspaces=[])

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog')
    def test_list_styles_of_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_styles.return_value = self.mock_styles

        # Execute
        response = self.engine.list_styles(
            workspace=self.workspace_name, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], str)

        # Test layer listed
        for n in self.style_names:
            self.assertIn(n, result)

        mc.get_styles.assert_called_with(workspaces=[self.workspace_name])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_list_styles_with_properties(self, mock_catalog):
        mc = mock_catalog()
        mc.get_styles.return_value = self.mock_styles

        # Execute
        response = self.engine.list_styles(with_properties=True)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Returns list
        self.assertIsInstance(result, list)

        # List of strings
        if len(result) > 0:
            self.assertIsInstance(result[0], dict)

        for r in result:
            self.assertIn("name", r)
            self.assertIn(r["name"], self.style_names)
            self.assertIn("workspace", r)
            self.assertEqual(self.workspace_name, r["workspace"])
        mc.get_styles.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_resource(self, mock_catalog):
        mc = mock_catalog()
        mc.get_default_workspace().name = self.workspace_name
        mc.get_resource.return_value = self.mock_resources[0]

        # Execute
        response = self.engine.get_resource(
            resource_id=self.resource_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn("name", r)
        self.assertIn(r["name"], self.resource_names)
        self.assertIn("workspace", r)
        self.assertEqual(self.workspace_name, r["workspace"])
        self.assertIn("store", r)
        self.assertEqual(self.store_name, r["store"])

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_resource_with_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        response = self.engine.get_resource(resource_id=resource_id, debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn("name", r)
        self.assertIn(r["name"], self.resource_names)
        self.assertIn("workspace", r)
        self.assertEqual(self.workspace_name, r["workspace"])
        self.assertIn("store", r)
        self.assertEqual(self.store_name, r["store"])

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_resource_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_resource(
            resource_id=self.resource_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # False
        self.assertFalse(response["success"])

        # Expect Error
        r = response["error"]

        # Properties
        self.assertIn("not found", r)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_resource_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_resource(
            resource_id=self.resource_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # False
        self.assertFalse(response["success"])

        # Expect Error
        r = response["error"]

        # Properties
        self.assertIn("Failed Request", r)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_resource_with_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Execute
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        response = self.engine.get_resource(
            resource_id=resource_id, store_id=self.store_name, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn("name", r)
        self.assertIn(r["name"], self.resource_names)
        self.assertIn("workspace", r)
        self.assertEqual(self.workspace_name, r["workspace"])
        self.assertIn("store", r)
        self.assertEqual(self.store_name, r["store"])

        mc.get_resource.assert_called_with(
            name=self.resource_names[0],
            store=self.store_name,
            workspace=self.workspace_name,
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_layer(self, mock_catalog, mock_get):
        mc = mock_catalog()
        mc.get_layer.return_value = self.mock_layers[0]

        mock_get.return_value = MockResponse(
            200, text="<GeoServerLayer><foo>bar</foo></GeoServerLayer>"
        )

        # Execute
        response = self.engine.get_layer(
            layer_id=self.layer_names[0], store_id=self.store_name, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn("name", r)
        self.assertEqual(self.layer_names[0], r["name"])
        self.assertIn("store", r)
        self.assertEqual(self.store_name, r["store"])
        self.assertIn("default_style", r)
        self.assertIn(self.default_style_name, r["default_style"])
        self.assertIn("styles", r)
        w_styles = [
            "{}:{}".format(self.workspace_name, style) for style in self.style_names
        ]
        for s in r["styles"]:
            self.assertIn(s, w_styles)

        self.assertIn("tile_caching", r)
        self.assertEqual({"foo": "bar"}, r["tile_caching"])

        mc.get_layer.assert_called_with(name=self.layer_names[0])
        mock_get.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_layer_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_layer(
            layer_id=self.layer_names[0], store_id=self.store_name, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("not found", r)

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_layer_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )

        # Execute
        response = self.engine.get_layer(
            layer_id=self.layer_names[0], store_id=self.store_name, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertEqual(r, "Failed Request")

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_layer_group(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.return_value = self.mock_layer_groups
        mc._return_first_item.return_value = self.mock_layer_groups[0]

        # Execute
        response = self.engine.get_layer_group(
            layer_group_id=self.layer_group_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # List of dictionaries
        self.assertIn("workspace", r)
        self.assertEqual(self.workspace_name, r["workspace"])
        self.assertIn("catalog", r)
        self.assertIn("layers", r)
        self.assertEqual(self.layer_names, r["layers"])
        self.assertNotIn("dom", r)

        mc.get_layergroups.assert_called_with(
            names=self.layer_group_names[0], workspaces=[]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_layer_group_with_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.return_value = self.mock_layer_groups
        mc._return_first_item.return_value = self.mock_layer_groups[0]
        layer_group_id = f"{self.workspace_name}:{self.layer_group_names[0]}"

        # Execute
        response = self.engine.get_layer_group(
            layer_group_id=layer_group_id, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # List of dictionaries
        self.assertIn("workspace", r)
        self.assertEqual(self.workspace_name, r["workspace"])
        self.assertIn("catalog", r)
        self.assertIn("layers", r)
        self.assertEqual(self.layer_names, r["layers"])
        self.assertNotIn("dom", r)

        mc.get_layergroups.assert_called_with(
            names=self.layer_group_names[0], workspaces=[self.workspace_name]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_layer_group_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.return_value = None
        mc._return_first_item.return_value = None

        # Execute
        response = self.engine.get_layer_group(
            layer_group_id=self.layer_group_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("not found", r)

        mc.get_layergroups.assert_called_with(
            names=self.layer_group_names[0], workspaces=[]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_layer_group_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroups.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )

        # Execute
        response = self.engine.get_layer_group(
            layer_group_id=self.layer_group_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertEqual(r, "Failed Request")

        mc.get_layergroups.assert_called_with(
            names=self.layer_group_names[0], workspaces=[]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn("name", r)
        self.assertIn(r["name"], self.store_names)
        self.assertIn("workspace", r)
        self.assertEqual(self.workspace_name, r["workspace"])

        mc.get_store.assert_called_with(
            name=self.store_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_store_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("Failed Request", r)

        mc.get_store.assert_called_with(
            name=self.store_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_store_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_store(store_id=self.store_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("not found", r)

        mc.get_store.assert_called_with(
            name=self.store_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_style(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.return_value = self.mock_styles[0]
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn("name", r)
        self.assertIn(r["name"], self.style_names)
        self.assertIn("workspace", r)
        self.assertEqual(self.workspace_name, r["workspace"])

        mc.get_style.assert_called_with(
            name=self.style_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_style_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.return_value = None
        mc.get_default_workspace().name = self.workspace_name

        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("not found", r)

        mc.get_style.assert_called_with(
            name=self.style_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_style_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_style.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )
        mc.get_default_workspace().name = self.workspace_name
        # Execute
        response = self.engine.get_style(style_id=self.style_names[0], debug=self.debug)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("Failed Request", r)

        mc.get_style.assert_called_with(
            name=self.style_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_get_layer_extent(self, mock_get):
        store_id = f"{self.workspace_name}:{self.store_name}"
        expected_bb = [-14.23, 28.1, -50.42, 89.18]
        jsondict = {
            "featureType": {
                "nativeBoundingBox": {
                    "minx": -12.23,
                    "miny": 22.1,
                    "maxx": -56.42,
                    "maxy": 32.18,
                },
                "latLonBoundingBox": {
                    "minx": -14.23,
                    "miny": 28.1,
                    "maxx": -50.42,
                    "maxy": 89.18,
                },
            }
        }

        mock_get.return_value = MockResponse(200, json=jsondict)
        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores/{datastore}/featuretypes/{feature_name}.json".format(  # noqa: E501
            endpoint=self.endpoint,
            workspace=self.workspace_name,
            datastore=self.store_name,
            feature_name="fee",
        )
        result = self.engine.get_layer_extent(store_id, "fee", buffer_factor=1.0)
        mock_get.assert_called_with(rest_endpoint, auth=self.auth)
        self.assertEqual(expected_bb, result)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_get_layer_extent_native(self, mock_workspace, mock_get):
        store_id = self.store_name
        mock_workspace().name = self.workspace_name
        expected_bb = [-12.23, 22.1, -56.42, 32.18]
        jsondict = {
            "featureType": {
                "nativeBoundingBox": {
                    "minx": -12.23,
                    "miny": 22.1,
                    "maxx": -56.42,
                    "maxy": 32.18,
                },
                "latLonBoundingBox": {
                    "minx": -14.23,
                    "miny": 28.1,
                    "maxx": -50.42,
                    "maxy": 89.18,
                },
            }
        }

        mock_get.return_value = MockResponse(200, json=jsondict)
        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores/{datastore}/featuretypes/{feature_name}.json".format(  # noqa: E501
            endpoint=self.endpoint,
            workspace=self.workspace_name,
            datastore=self.store_name,
            feature_name="fee",
        )
        result = self.engine.get_layer_extent(
            store_id, "fee", native=True, buffer_factor=1.0
        )
        mock_get.assert_called_with(rest_endpoint, auth=self.auth)
        self.assertEqual(expected_bb, result)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_get_layer_extent_feature_bbox_none(self, mock_get):
        store_id = f"{self.workspace_name}:{self.store_name}"
        expected_bb = [-128.583984375, 22.1874049914, -64.423828125, 52.1065051908]
        jsondict = {}
        mock_get.return_value = MockResponse(200, json=jsondict)
        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores/{datastore}/featuretypes/{feature_name}.json".format(  # noqa: E501
            endpoint=self.endpoint,
            workspace=self.workspace_name,
            datastore=self.store_name,
            feature_name="fee",
        )
        result = self.engine.get_layer_extent(store_id, "fee", buffer_factor=1.0)
        mock_get.assert_called_with(rest_endpoint, auth=self.auth)
        self.assertEqual(expected_bb, result)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_get_layer_extent_not_200(self, mock_get, mock_logger):
        store_id = f"{self.workspace_name}:{self.store_name}"
        mock_get.return_value = MockResponse(500)
        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores/{datastore}/featuretypes/{feature_name}.json".format(  # noqa: E501
            endpoint=self.endpoint,
            workspace=self.workspace_name,
            datastore=self.store_name,
            feature_name="fee",
        )
        self.assertRaises(
            requests.RequestException,
            self.engine.get_layer_extent,
            store_id,
            "fee",
            buffer_factor=1.0,
        )
        mock_get.assert_called_with(rest_endpoint, auth=self.auth)
        mock_logger.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.return_value = self.mock_workspaces[0]

        # Execute
        response = self.engine.get_workspace(
            workspace_id=self.workspace_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Properties
        self.assertIn("name", r)
        self.assertIn(r["name"], self.workspace_names[0])

        mc.get_workspace.assert_called_with(name=self.workspace_names[0])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_workspace_none(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.return_value = None

        # Execute
        response = self.engine.get_workspace(
            workspace_id=self.workspace_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("not found", r)

        mc.get_workspace.assert_called_with(name=self.workspace_names[0])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_get_workspace_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )

        # Execute
        response = self.engine.get_workspace(
            workspace_id=self.workspace_names[0], debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("Failed Request", r)

        mc.get_workspace.assert_called_with(name=self.workspace_names[0])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_resource(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            title="foo", geometry="points"
        )

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = "lines"

        # Execute
        response = self.engine.update_resource(
            resource_id=resource_id,
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
        )
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["title"], new_title)
        self.assertEqual(result["geometry"], new_geometry)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_resource_no_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            title="foo", geometry="points"
        )
        mc.get_default_workspace().name = self.workspace_name

        # Setup
        resource_id = self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = "lines"

        # Execute
        response = self.engine.update_resource(
            resource_id=resource_id,
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
        )
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["title"], new_title)
        self.assertEqual(result["geometry"], new_geometry)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_resource_style(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            styles=["style_name"],
        )
        mc.get_style.side_effect = mock_get_style

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_styles = ["new_style_name"]

        # Execute
        response = self.engine.update_resource(
            resource_id=resource_id, styles=new_styles, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["styles"], new_styles)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_resource_style_colon(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            styles=["1:2"],
        )
        mc.get_style.side_effect = mock_get_style

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_styles = ["11:22"]

        # Execute
        response = self.engine.update_resource(
            resource_id=resource_id, styles=new_styles, debug=self.debug
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["styles"], new_styles)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_resource_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = "lines"

        # Execute
        response = self.engine.update_resource(
            resource_id=resource_id,
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Fail
        self.assertFalse(response["success"])

        # Expect Error
        r = response["error"]

        # Properties
        self.assertIn("Failed Request", r)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0], store=None, workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_resource_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = mock.NonCallableMagicMock(
            store=self.store_name, title="foo", geometry="points"
        )

        # Setup
        resource_id = self.workspace_name + ":" + self.resource_names[0]
        new_title = random_string_generator(15)
        new_geometry = "lines"

        # Execute
        response = self.engine.update_resource(
            resource_id=resource_id,
            store=self.store_name,
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
        )
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["title"], new_title)
        self.assertEqual(result["geometry"], new_geometry)
        self.assertEqual(result["store"], self.store_name)

        mc.get_resource.assert_called_with(
            name=self.resource_names[0],
            store=self.store_name,
            workspace=self.workspace_name,
        )
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_layer(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0], title="foo", geometry="points"
        )

        # Setup
        new_title = random_string_generator(15)
        new_geometry = "lines"

        # Execute
        response = self.engine.update_layer(
            layer_id=self.layer_names[0],
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
        )
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["title"], new_title)
        self.assertEqual(result["geometry"], new_geometry)

        mc.get_layer.assert_called_with(name=self.layer_names[0])
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_layer_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layer.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0], title="foo", geometry="points"
        )

        # Setup
        new_title = random_string_generator(15)
        new_geometry = "lines"

        # Execute
        response = self.engine.update_layer(
            layer_id=self.layer_names[0],
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
        )
        # Validate response object
        self.assert_valid_response_object(response)

        # Fail
        self.assertFalse(response["success"])

        # Expect Error
        r = response["error"]

        # Properties
        self.assertIn("Failed Request", r)

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_layer_with_tile_caching_params(self, mock_catalog, mock_post):
        mc = mock_catalog()
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0], title="foo", geometry="points"
        )
        mock_post.return_value = MockResponse(200)

        # Setup
        new_title = random_string_generator(15)
        new_geometry = "lines"
        tile_caching = {"foo": "bar"}

        # Execute
        response = self.engine.update_layer(
            layer_id=self.layer_names[0],
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
            tile_caching=tile_caching,
        )
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["title"], new_title)
        self.assertEqual(result["geometry"], new_geometry)
        self.assertIn("foo", result["tile_caching"])
        self.assertEqual(result["tile_caching"]["foo"], "bar")

        mc.get_layer.assert_called_with(name=self.layer_names[0])
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_layer_with_tile_caching_params_not_200(
        self, mock_catalog, mock_post
    ):
        mc = mock_catalog()
        mc.get_layer.return_value = mock.NonCallableMagicMock(
            name=self.layer_names[0], title="foo", geometry="points"
        )
        mock_post.return_value = MockResponse(500, text="server error")

        # Setup
        new_title = random_string_generator(15)
        new_geometry = "lines"
        tile_caching = {"foo": "bar"}

        # Execute
        response = self.engine.update_layer(
            layer_id=self.layer_names[0],
            title=new_title,
            geometry=new_geometry,
            debug=self.debug,
            tile_caching=tile_caching,
        )
        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])

        # Extract Result
        self.assertIn("server error", response["error"])

        mc.get_layer.assert_called_with(name=self.layer_names[0])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_layer_group(self, mock_catalog):
        mc = mock_catalog()
        mock_layer_group = mock.NonCallableMagicMock(layers=self.layer_names)
        mock_layer_group.name = self.layer_group_names[0]
        mc.get_layergroup.return_value = mock_layer_group

        # Setup
        new_layers = random_string_generator(15)

        # Execute
        response = self.engine.update_layer_group(
            layer_group_id=self.layer_group_names[0],
            layers=new_layers,
            debug=self.debug,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        result = response["result"]

        # Properties
        self.assertEqual(result["layers"], new_layers)

        mc.get_layergroup.assert_called_with(
            name=self.layer_group_names[0], workspace=None
        )
        mc.save.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_update_layer_group_failed_request_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_layergroup.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )

        # Setup
        new_layers = random_string_generator(15)

        # Execute
        response = self.engine.update_layer_group(
            layer_group_id=self.mock_layer_groups[0],
            layers=new_layers,
            debug=self.debug,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Fail
        self.assertFalse(response["success"])

        # Expect Error
        r = response["error"]

        # Properties
        self.assertIn("Failed Request", r)

        mc.get_layergroup.assert_called_with(
            name=self.mock_layer_groups[0], workspace=None
        )

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.list_styles"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_update_layer_styles(
        self, mock_ws, mock_list_styles, mock_put, mock_logger, mock_get_layer
    ):
        mock_put.return_value = MockResponse(200)
        mock_get_layer.return_value = {"success": True, "result": None}
        mock_ws().name = self.workspace_name
        mock_list_styles.return_value = self.style_names
        layer_id = self.layer_names[0]
        default_style = self.style_names[0]
        other_styles = [self.style_names[1]]

        self.engine.update_layer_styles(layer_id, default_style, other_styles)

        expected_url = "{endpoint}layers/{layer}.xml".format(
            endpoint=self.endpoint, layer=layer_id
        )

        expected_headers = {"Content-type": "text/xml"}

        with open(os.path.join(self.files_root, "test_create_layer.xml")) as rendered:
            expected_xml = rendered.read()

        mock_put.assert_called_with(
            expected_url, headers=expected_headers, auth=self.auth, data=expected_xml
        )
        mock_logger.info.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.list_styles"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_update_layer_styles_exception(
        self, mock_ws, mock_list_styles, mock_put, mock_logger
    ):
        mock_put.return_value = MockResponse(500, "500 exception")
        mock_ws().name = self.workspace_name
        mock_list_styles.return_value = self.style_names
        layer_id = self.layer_names[0]
        default_style = self.style_names[0]
        other_styles = [self.style_names[1]]

        self.assertRaises(
            requests.RequestException,
            self.engine.update_layer_styles,
            layer_id,
            default_style,
            other_styles,
        )

        mock_logger.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_delete_resource_with_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        resource_id = "{}:{}".format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id, store_id=self.mock_store)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])
        mc.get_resource.assert_called_with(
            name=self.resource_names[0],
            store=self.mock_store,
            workspace=self.workspace_name,
        )
        mc.delete.assert_called_with(
            config_object=self.mock_resources[0], purge=False, recurse=False
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
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
        self.assertTrue(response["success"])
        mc.get_resource.assert_called_with(
            name=self.resource_names[0],
            store=self.mock_store,
            workspace=self.workspace_name,
        )
        mc.delete.assert_called_with(
            config_object=self.mock_resources[0], purge=False, recurse=False
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_delete_resource_error(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]
        mc.delete.side_effect = geoserver.catalog.FailedRequestError()

        resource_id = "{}:{}".format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id, store_id=self.mock_store)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])
        mc.delete.assert_called_with(
            config_object=self.mock_resources[0], purge=False, recurse=False
        )
        mc.get_resource.assert_called_with(
            name=self.resource_names[0],
            store=self.mock_store,
            workspace=self.workspace_name,
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_delete_resource_does_not_exist(self, mock_catalog):
        mc = mock_catalog()
        mc.get_resource.return_value = None

        resource_id = "{}:{}".format(self.workspace_name, self.resource_names[0])

        # Execute
        response = self.engine.delete_resource(resource_id, store_id=self.store_name)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertFalse(response["success"])
        self.assertIn("GeoServer object does not exist", response["error"])
        mc.get_resource.assert_called_with(
            name=self.resource_names[0],
            store=self.store_name,
            workspace=self.workspace_name,
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_delete_layer(self, mock_workspace, mock_delete):
        mock_delete.return_value = MockResponse(200)
        mock_workspace().name = self.workspace_name
        layer_name = self.layer_names[0]

        # Execute
        response = self.engine.delete_layer(layer_name, datastore=self.store_name)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    def test_delete_layer_warning(self, mock_delete, mock_logger):
        mock_delete.return_value = MockResponse(404)
        layer_name = f"{self.workspace_name}:{self.layer_names[0]}"

        # Execute
        self.engine.delete_layer(layer_name, datastore=self.store_name)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    def test_delete_layer_exception(self, mock_delete, mock_logger):
        mock_delete.return_value = MockResponse(500, "500 exception")
        layer_name = f"{self.workspace_name}:{self.layer_names[0]}"

        # Execute
        self.assertRaises(
            requests.RequestException,
            self.engine.delete_layer,
            layer_name,
            datastore=self.store_name,
        )
        mock_logger.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    def test_delete_layer_group(self, mock_delete):
        mock_delete.return_value = MockResponse(200)
        group_name = f"{self.workspace_name}:{self.layer_group_names[0]}"

        self.engine.delete_layer_group(group_name)

        # Validate endpoint calls
        url = "{endpoint}workspaces/{w}/layergroups/{lg}".format(
            endpoint=self.endpoint, w=self.workspace_name, lg=self.layer_group_names[0]
        )

        # Create feature type call
        mock_delete.assert_called_with(url, auth=self.auth, params={'recurse': 'true'})

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_delete_layer_group_no_group(self, mock_workspace, mock_delete):
        mock_delete.return_value = MockResponse(404, "No such layer group")
        mock_workspace().name = self.workspace_name
        group_name = self.layer_group_names[0]

        self.engine.delete_layer_group(group_name)

        # Validate endpoint calls
        url = "{endpoint}workspaces/{w}/layergroups/{lg}".format(
            endpoint=self.endpoint, w=self.workspace_name, lg=self.layer_group_names[0]
        )

        # Create feature type call
        mock_delete.assert_called_with(url, auth=self.auth, params={'recurse': 'true'})

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_delete_layer_group_exception(
        self, mock_workspace, mock_delete, mock_logger
    ):
        mock_delete.return_value = MockResponse(
            404, "These aren't the droids you're looking for..."
        )
        mock_workspace().name = self.workspace_name
        group_name = self.layer_group_names[0]

        self.assertRaises(
            requests.RequestException, self.engine.delete_layer_group, group_name
        )
        mock_logger.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_delete_workspace(self, mock_catalog):
        mc = mock_catalog()
        mc.get_workspace.return_value = self.mock_workspaces[0]

        # Do delete
        response = self.engine.delete_workspace(workspace_id=self.workspace_names[0])

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response["success"])
        self.assertIsNone(response["result"])

        mc.get_workspace.assert_called_with(self.workspace_names[0])
        mc.delete.assert_called_with(
            config_object=self.mock_workspaces[0], purge=False, recurse=False
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_delete_store(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.return_value = self.mock_stores[0]
        mc.get_default_workspace().name = self.workspace_name

        # Do delete
        response = self.engine.delete_store(store_id=self.store_names[0])

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response["success"])
        self.assertIsNone(response["result"])

        mc.get_store.assert_called_with(
            name=self.store_names[0], workspace=self.workspace_name
        )
        mc.delete.assert_called_with(
            config_object=self.mock_stores[0], purge=False, recurse=False
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_delete_store_failed_request(self, mock_catalog):
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError(
            "Failed Request"
        )

        mc.get_default_workspace().name = self.workspace_name

        # Do delete
        response = self.engine.delete_store(store_id=self.store_names[0])

        # Failure Check
        self.assert_valid_response_object(response)
        self.assertFalse(response["success"])
        self.assertIn("Failed Request", response["error"])

        mc.get_store.assert_called_with(
            name=self.store_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_delete_coverage_store(self, mock_ws, mock_delete):
        mock_delete.return_value = MockResponse(200)
        mock_ws().name = self.workspace_name

        coverage_name = "foo"
        url = "workspaces/{workspace}/coveragestores/{coverage_store_name}".format(
            workspace=self.workspace_name,
            coverage_store_name=coverage_name,
        )

        json = {"recurse": True, "purge": True}

        self.engine.delete_coverage_store(store_id=coverage_name)
        put_call_args = mock_delete.call_args_list
        self.assertIn(url, put_call_args[0][1]["url"])
        self.assertEqual(json, put_call_args[0][1]["params"])
        self.assertEqual(
            {"Content-type": "application/json"}, put_call_args[0][1]["headers"]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    def test_delete_coverage_store_with_warning(self, mock_delete, mock_log):
        mock_delete.return_value = MockResponse(403)

        coverage_name = f"{self.workspace_name}:foo"
        url = "workspaces/{workspace}/coveragestores/{coverage_store_name}".format(
            workspace=self.workspace_name,
            coverage_store_name="foo",
        )

        json = {"recurse": True, "purge": True}

        self.engine.delete_coverage_store(store_id=coverage_name)

        put_call_args = mock_delete.call_args_list
        self.assertIn(url, put_call_args[0][1]["url"])
        self.assertEqual(json, put_call_args[0][1]["params"])
        self.assertEqual(
            {"Content-type": "application/json"}, put_call_args[0][1]["headers"]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    def test_delete_coverage_store_with_error(self, mock_delete, mock_log):
        mock_delete.return_value = MockResponse(500)

        coverage_name = f"{self.workspace_name}:foo"
        url = "workspaces/{workspace}/coveragestores/{coverage_store_name}".format(
            workspace=self.workspace_name,
            coverage_store_name="foo",
        )

        json = {"recurse": True, "purge": True}

        self.assertRaises(
            requests.RequestException, self.engine.delete_coverage_store, coverage_name
        )

        put_call_args = mock_delete.call_args_list
        self.assertIn(url, put_call_args[0][1]["url"])
        self.assertEqual(json, put_call_args[0][1]["params"])
        self.assertEqual(
            {"Content-type": "application/json"}, put_call_args[0][1]["headers"]
        )

        mock_log.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_delete_style(self, mock_workspace, mock_delete):
        mock_workspace.return_value = self.mock_workspaces[0]
        mock_delete.return_value = MockResponse(200)
        style_id = "{}:{}".format(
            self.mock_workspaces[0].name, self.mock_styles[0].name
        )

        # Do delete
        response = self.engine.delete_style(style_id=style_id)

        # Should succeed
        self.assert_valid_response_object(response)
        self.assertTrue(response["success"])
        self.assertIsNone(response["result"])

        # Delete Tests
        delete_call_args = mock_delete.call_args_list
        expected_url = "{endpoint}workspaces/{w}/styles/{s}".format(
            endpoint=self.endpoint,
            w=self.mock_workspaces[0].name,
            s=self.mock_styles[0].name,
        )
        expected_headers = {"Content-type": "application/json"}
        expected_params = {"purge": False}
        self.assertEqual(expected_url, delete_call_args[0][1]["url"])
        self.assertEqual(self.auth, delete_call_args[0][1]["auth"])
        self.assertEqual(expected_headers, delete_call_args[0][1]["headers"])
        self.assertEqual(expected_params, delete_call_args[0][1]["params"])

        mock_delete.assert_called_with(url=expected_url, auth=self.auth, headers=expected_headers,
                                       params=expected_params)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    def test_delete_style_warning(self, mock_delete, mock_logger):
        mock_delete.return_value = mock.MagicMock(status_code=404)
        style_id = self.mock_styles[0].name

        self.engine.delete_style(style_id=style_id, purge=True)

        # Validate endpoint calls
        url = "{endpoint}styles/{s}".format(endpoint=self.endpoint, s=style_id)

        headers = {"Content-type": "application/json"}

        params = {"purge": True}

        # Create feature type call
        mock_delete.assert_called_with(
            url=url, auth=self.auth, headers=headers, params=params
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.delete")
    def test_delete_style_exception(self, mock_delete, mock_logger):
        mock_delete.return_value = mock.MagicMock(status_code=500)
        style_id = self.mock_styles[0].name

        self.assertRaises(requests.RequestException, self.engine.delete_style, style_id)

        # Validate endpoint calls
        url = "{endpoint}styles/{s}".format(endpoint=self.endpoint, s=style_id)

        headers = {"Content-type": "application/json"}

        params = {"purge": False}

        # Create feature type call
        mock_delete.assert_called_with(
            url=url, auth=self.auth, headers=headers, params=params
        )
        mock_logger.error.assert_called()

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer_group"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_layer_group(self, mock_post, mock_get_layer_group):
        mock_post.return_value = MockResponse(201)
        group_name = f"{self.workspace_name}:{self.layer_group_names[0]}"
        layer_names = self.layer_names[:2]
        default_styles = self.style_names
        self.engine.create_layer_group(group_name, layer_names, default_styles)

        # Validate endpoint calls
        layer_group_url = "workspaces/{w}/layergroups.json".format(
            w=self.workspace_name
        )
        with open(
            os.path.join(self.files_root, "test_create_layer_group.xml")
        ) as rendered:
            expected_xml = rendered.read()

        # Create feature type call
        post_call_args = mock_post.call_args_list
        # call_args[call_num][0=args|1=kwargs][arg_index|kwarg_key]
        self.assertIn(layer_group_url, post_call_args[0][0][0])
        self.assertEqual(expected_xml, post_call_args[0][1]["data"])
        mock_get_layer_group.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_create_layer_group_exception(self, mock_workspace, mock_post, mock_logger):
        mock_post.return_value = MockResponse(500, "Layer group exception")
        mock_workspace().name = self.workspace_name
        group_name = self.layer_group_names[0]
        layer_names = self.layer_names[:2]
        default_styles = self.style_names
        with self.assertRaises(requests.RequestException) as error:
            self.engine.create_layer_group(group_name, layer_names, default_styles)
        self.assertEqual(
            "Create Layer Group Status Code 500: Layer group exception",
            str(error.exception),
        )
        mock_logger.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_default_workspace().name = self.workspace_name[0]
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_name = os.path.join(self.files_root, "shapefile", "test")
        store_id = self.store_names[0]

        # Execute
        response = self.engine.create_shapefile_resource(
            store_id=store_id, shapefile_base=shapefile_name, overwrite=True
        )
        # Should succeed
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r["name"])
        self.assertIn(self.store_name[0], r["store"])

        mc.get_default_workspace.assert_called_with()
        mc.get_resource.assert_called_with(
            store=self.store_names[0],
            workspace=self.workspace_name[0],
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource_zipfile(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_name = os.path.join(self.files_root, "shapefile", "test1.zip")
        # Workspace is given
        store_id = "{}:{}".format(self.workspace_name, self.store_names[0])

        # Execute
        response = self.engine.create_shapefile_resource(
            store_id=store_id,
            shapefile_zip=shapefile_name,
            overwrite=True,
            charset="ISO - 8559 - 1",
        )
        # Should succeed
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r["name"])
        self.assertIn(self.store_name[0], r["store"])

        mc.get_resource.assert_called_with(
            store=self.store_names[0], workspace=self.workspace_name
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource_upload(self, mock_catalog, mock_put):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_cst = os.path.join(self.files_root, "shapefile", "test.cst")
        shapefile_dbf = os.path.join(self.files_root, "shapefile", "test.dbf")
        shapefile_prj = os.path.join(self.files_root, "shapefile", "test.prj")
        shapefile_shp = os.path.join(self.files_root, "shapefile", "test.shp")
        shapefile_shx = os.path.join(self.files_root, "shapefile", "test.shx")

        # Workspace is given
        store_id = "{}:{}".format(self.workspace_name, self.store_names[0])

        with open(shapefile_cst, 'rb') as cst_upload, \
                open(shapefile_dbf, 'rb') as dbf_upload, \
                open(shapefile_prj, 'rb') as prj_upload, \
                open(shapefile_shp, 'rb') as shp_upload, \
                open(shapefile_shx, 'rb') as shx_upload:
            upload_list = [cst_upload, dbf_upload, prj_upload, shp_upload, shx_upload]
            response = self.engine.create_shapefile_resource(
                store_id=store_id,
                shapefile_upload=upload_list,
                overwrite=True,
            )
        # Should succeed
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r["name"])
        self.assertIn(self.store_name[0], r["store"])

        mc.get_resource.assert_called_with(
            store=self.store_names[0],
            workspace=self.workspace_name,
        )

    def test_create_shapefile_resource_zipfile_typeerror(self):
        # Setup
        shapefile_name = os.path.join(self.files_root, "shapefile", "test.shp")
        # Workspace is given
        store_id = "{}:{}".format(self.workspace_name, self.store_name[0])

        # Should Fail
        self.assertRaises(
            TypeError,
            self.engine.create_shapefile_resource,
            store_id=store_id,
            shapefile_zip=shapefile_name,
            overwrite=True,
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource_overwrite_store_exists(self, mock_catalog):
        # Setup
        shapefile_name = os.path.join(self.files_root, "shapefile", "test")
        store_id = "{}:{}".format(self.workspace_name, self.store_names[0])

        # Execute
        response = self.engine.create_shapefile_resource(
            store_id=store_id, shapefile_base=shapefile_name, overwrite=False
        )
        # Should Fail
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        # Check error message
        error_message = (
            "There is already a store named "
            + self.store_names[0]
            + " in "
            + self.workspace_name
        )
        self.assertIn(error_message, r)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource_overwrite_store_not_exists(
        self, mock_catalog, mock_put
    ):
        mock_put.return_value = MockResponse(201)
        mc = mock_catalog()
        mc.get_store.side_effect = geoserver.catalog.FailedRequestError()
        mc.get_resource.return_value = self.mock_resources[0]

        # Setup
        shapefile_name = os.path.join(self.files_root, "shapefile", "test")
        # Workspace is given
        store_id = "{}:{}".format(self.workspace_name, self.store_names[0])

        # Execute
        response = self.engine.create_shapefile_resource(
            store_id=store_id, shapefile_base=shapefile_name, overwrite=False
        )
        # Should succeed
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)
        self.assertIn(self.mock_resources[0].name, r["name"])
        self.assertIn(self.store_name[0], r["store"])

        mc.get_resource.assert_called_with(
            store=self.store_names[0],
            workspace=self.workspace_name,
        )

    def test_create_shapefile_resource_validate_shapefile_args(self):
        self.assertRaises(
            ValueError, self.engine.create_shapefile_resource, store_id="foo"
        )
        self.assertRaises(
            ValueError,
            self.engine.create_shapefile_resource,
            store_id="foo",
            shapefile_zip="zipfile",
            shapefile_upload="su",
            shapefile_base="base",
        )
        self.assertRaises(
            ValueError,
            self.engine.create_shapefile_resource,
            store_id="foo",
            shapefile_upload="su",
            shapefile_base="base",
        )
        self.assertRaises(
            ValueError,
            self.engine.create_shapefile_resource,
            store_id="foo",
            shapefile_zip="zipfile",
            shapefile_base="base",
        )
        self.assertRaises(
            ValueError,
            self.engine.create_shapefile_resource,
            store_id="foo",
            shapefile_zip="zipfile",
            shapefile_upload="su",
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource_failure(self, _, mock_put):
        mock_put.return_value = MockResponse(404, reason="Failure")

        # Setup
        shapefile_name = os.path.join(self.files_root, "shapefile", "test")
        store_id = "{}:{}".format(self.workspace_name, self.store_name[0])

        # Execute
        response = self.engine.create_shapefile_resource(
            store_id=store_id, shapefile_base=shapefile_name, overwrite=True
        )
        # Should succeed
        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        # Check Response
        self.assertIn("404", r)
        self.assertIn("Failure", r)

    def test_type_property(self):
        response = self.engine.type
        expected_response = "GEOSERVER"

        # Check Response
        self.assertEqual(response, expected_response)

    def test_public_endpoint_property(self):
        response = self.engine.public_endpoint

        # Check Response
        self.assertIn(".public.", response)

    def test_gwc_endpoint_property(self):
        response = self.engine.gwc_endpoint

        # Check Response
        self.assertIn("/gwc/rest/", response)

    def test_get_gwc_endpoint(self):
        response = self.engine.get_gwc_endpoint(public=False)

        # Check Response
        self.assertIn("/gwc/rest/", response)

        mock_engine = GeoServerSpatialDatasetEngine(
            endpoint=self.endpoint,
            username=self.username,
            password=self.password,
            public_endpoint=self.public_endpoint[:-1],
        )
        response = mock_engine.get_gwc_endpoint()

        # Check Response with public endpoint
        self.assertIn(".public.", response)
        self.assertIn("/gwc/rest/", response)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource_default_style_success(self, mock_catalog, mock_put):
        # first PUT = shapefile upload (201), second PUT = set default style (200)
        mock_put.side_effect = [MockResponse(201), MockResponse(200)]
        mc = mock_catalog()
        mc.get_default_workspace().name = self.workspace_name[0]
        mc.get_resource.return_value = self.mock_resources[0]

        shapefile_base = os.path.join(self.files_root, "shapefile", "test")
        store_id = self.store_names[0]

        resp = self.engine.create_shapefile_resource(
            store_id=store_id,
            shapefile_base=shapefile_base,
            overwrite=True,
            default_style="points",
        )

        self.assertTrue(resp["success"])
        # ensure we hit the second PUT with the layer XML body
        self.assertEqual(2, len(mock_put.call_args_list))
        second_call = mock_put.call_args_list[1]
        self.assertIn("<defaultStyle>", second_call.kwargs["data"])
        self.assertIn("<name>points</name>", second_call.kwargs["data"])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_shapefile_resource_default_style_failure(self, mock_catalog, mock_put):
        # first PUT = shapefile upload ok, second PUT = layer update fails
        mock_put.side_effect = [MockResponse(201), MockResponse(500, text="bad", reason="Oops")]
        mc = mock_catalog()
        mc.get_default_workspace().name = self.workspace_name[0]
        mc.get_resource.return_value = self.mock_resources[0]

        shapefile_base = os.path.join(self.files_root, "shapefile", "test")
        store_id = self.store_names[0]

        resp = self.engine.create_shapefile_resource(
            store_id=store_id,
            shapefile_base=shapefile_base,
            overwrite=True,
            default_style="points",
        )

        self.assertFalse(resp["success"])
        self.assertIn("Oops(500): bad", resp["error"])

    def test_get_ows_endpoint(self):
        workspace = self.workspace_name
        response = self.engine.get_ows_endpoint(workspace, public=False)

        expected_url_match = "/{ws}/ows/".format(ws=workspace)

        # Check Response
        self.assertIn(expected_url_match, response)

        mock_engine = GeoServerSpatialDatasetEngine(
            endpoint=self.endpoint,
            username=self.username,
            password=self.password,
            public_endpoint=self.public_endpoint[:-1],
        )
        response = mock_engine.get_ows_endpoint(workspace)

        # Check Response with public endpoint
        self.assertIn(".public.", response)
        self.assertIn(expected_url_match, response)

    def test_get_wms_endpoint(self):
        response = self.engine.get_wms_endpoint(public=False)

        # Check Response
        self.assertIn("/wms/", response)

        mock_engine = GeoServerSpatialDatasetEngine(
            endpoint=self.endpoint,
            username=self.username,
            password=self.password,
            public_endpoint=self.public_endpoint[:-1],
        )
        response = mock_engine.get_wms_endpoint()

        # Check Response with public endpoint
        self.assertIn(".public.", response)
        self.assertIn("/wms/", response)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_reload_ports_none(self, mock_post):
        mock_post.return_value = MockResponse(200)
        self.engine.reload()
        rest_endpoint = self.public_endpoint + "reload"
        mock_post.assert_called_with(rest_endpoint, auth=self.auth)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_reload_with_ports(self, mock_post):
        mock_post.return_value = MockResponse(200)
        self.engine.reload([17300, 18000])
        self.assertEqual(mock_post.call_count, 2)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_reload_not_200(self, mock_post, mock_logger):
        mock_post.return_value = MockResponse(500, "500 exception")
        response = self.engine.reload()
        mock_logger.error.assert_called()
        self.assertEqual(
            "Catalog Reload Status Code 500: 500 exception", response["error"][0]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_reload_connection_error(self, mock_post, mock_logger):
        mock_post.side_effect = requests.ConnectionError()
        self.engine.reload()
        mock_logger.warning.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_gwc_reload_ports_none(self, mock_post):
        mock_post.return_value = MockResponse(200)
        self.engine.gwc_reload()
        rest_endpoint = self.public_endpoint.replace("rest", "gwc/rest") + "reload"
        mock_post.assert_called_with(rest_endpoint, auth=self.auth)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_gwc_reload_with_ports(self, mock_post):
        mock_post.return_value = MockResponse(200)
        self.engine.gwc_reload([17300, 18000])
        self.assertEqual(mock_post.call_count, 2)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_gwc_reload_not_200(self, mock_post, mock_logger):
        mock_post.return_value = MockResponse(500, "500 exception")
        response = self.engine.gwc_reload()
        mock_logger.error.assert_called()
        self.assertEqual(
            "GeoWebCache Reload Status Code 500: 500 exception", response["error"][0]
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_gwc_reload_connection_error(self, mock_post, mock_logger):
        mock_post.side_effect = requests.ConnectionError()
        self.engine.gwc_reload()
        mock_logger.warning.assert_called()

    def test_ini_no_slash_endpoint(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint="http://localhost:8181/geoserver/rest",
            username=self.username,
            password=self.password,
        )

        expected_endpoint = "http://localhost:8181/geoserver/gwc/rest/"

        # Check Response
        self.assertEqual(expected_endpoint, self.engine.gwc_endpoint)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_validate(self, mock_get):
        # Missing Schema
        mock_get.side_effect = requests.exceptions.MissingSchema
        self.assertRaises(AssertionError, self.engine.validate)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_validate_401(self, mock_get):
        # 401 Code
        mock_get.return_value = MockResponse(401)
        self.assertRaises(AssertionError, self.engine.validate)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_validate_not_200(self, mock_get):
        # !201 Code
        mock_get.return_value = MockResponse(201)

        self.assertRaises(AssertionError, self.engine.validate)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_validate_not_geoserver(self, mock_get):
        # text
        mock_get.return_value = MockResponse(200, text="Bad text")
        self.assertRaises(AssertionError, self.engine.validate)

    def test_modify_tile_cache_invalid_operation(self):
        layer_id = f"{self.workspace_name}:gwc_layer_name"
        operation = "invalid-operation"
        self.assertRaises(
            ValueError, self.engine.modify_tile_cache, layer_id, operation
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_modify_tile_cache_mass_truncate(self, mock_ws, mock_post, mock_logger):
        mock_post.return_value = mock.MagicMock(status_code=200)
        mock_ws().name = self.workspace_name
        layer_id = "gwc_layer_name"
        operation = self.engine.GWC_OP_MASS_TRUNCATE
        self.engine.modify_tile_cache(layer_id, operation)

        url = "masstruncate/"

        # Create feature type call
        post_call_args = mock_post.call_args_list
        # call_args[call_num][0=args|1=kwargs][arg_index|kwarg_key]
        self.assertIn(url, post_call_args[0][0][0])
        mock_logger.info.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_modify_tile_cache_seed(self, mock_post, mock_logger):
        mock_post.return_value = mock.MagicMock(status_code=200)
        layer_id = f"{self.workspace_name}:gwc_layer_name"
        operation = self.engine.GWC_OP_SEED
        self.engine.modify_tile_cache(layer_id, operation)

        url = "seed/{workspace}:{name}.xml".format(
            workspace=self.workspace_name, name="gwc_layer_name"
        )

        # Create feature type call
        post_call_args = mock_post.call_args_list
        # call_args[call_num][0=args|1=kwargs][arg_index|kwarg_key]
        self.assertIn(url, post_call_args[0][0][0])
        self.assertIn(operation, post_call_args[0][1]["data"])
        mock_logger.info.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_modify_tile_cache_reseed(self, mock_post, mock_logger):
        mock_post.return_value = mock.MagicMock(status_code=200)
        layer_id = f"{self.workspace_name}:gwc_layer_name"
        operation = self.engine.GWC_OP_RESEED
        self.engine.modify_tile_cache(layer_id, operation)

        url = "seed/{workspace}:{name}.xml".format(
            workspace=self.workspace_name, name="gwc_layer_name"
        )

        # Create feature type call
        post_call_args = mock_post.call_args_list
        # call_args[call_num][0=args|1=kwargs][arg_index|kwarg_key]
        self.assertIn(url, post_call_args[0][0][0])
        self.assertIn(operation, post_call_args[0][1]["data"])
        mock_logger.info.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_modify_tile_cache_exception(self, mock_post, mock_logger):
        mock_post.return_value = mock.MagicMock(status_code=500)
        layer_id = f"{self.workspace_name}:gwc_layer_name"
        operation = self.engine.GWC_OP_MASS_TRUNCATE
        self.assertRaises(
            requests.RequestException,
            self.engine.modify_tile_cache,
            layer_id,
            operation,
        )

        url = "masstruncate/"

        # Create feature type call
        post_call_args = mock_post.call_args_list
        # call_args[call_num][0=args|1=kwargs][arg_index|kwarg_key]
        self.assertIn(url, post_call_args[0][0][0])
        mock_logger.error.assert_called()

    def test_terminate_tile_cache_tasks_invalid_operation(self):
        layer_id = f"{self.workspace_name}:gwc_layer_name"
        operation = "invalid-operation"
        self.assertRaises(
            ValueError, self.engine.terminate_tile_cache_tasks, layer_id, kill=operation
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_terminate_tile_cache_tasks(self, mock_ws, mock_post):
        mock_post.return_value = mock.MagicMock(status_code=200)
        mock_ws().name = self.workspace_name
        layer_id = "gwc_layer_name"

        self.engine.terminate_tile_cache_tasks(layer_id)

        url = "{endpoint}seed/{workspace}:{name}".format(
            endpoint=self.engine.get_gwc_endpoint(),
            workspace=self.workspace_name,
            name=layer_id,
        )

        # Create feature type call
        mock_post.assert_called_with(
            url, auth=self.auth, data={"kill_all": self.engine.GWC_KILL_ALL}
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_terminate_tile_cache_tasks_exception(self, mock_post):
        mock_post.return_value = mock.MagicMock(status_code=500)
        layer_id = f"{self.workspace_name}:gwc_layer_name"

        self.assertRaises(
            requests.RequestException, self.engine.terminate_tile_cache_tasks, layer_id
        )

        url = "{endpoint}seed/{workspace}:{name}".format(
            endpoint=self.engine.get_gwc_endpoint(),
            workspace=self.workspace_name,
            name="gwc_layer_name",
        )

        # Create feature type call
        mock_post.assert_called_with(
            url, auth=self.auth, data={"kill_all": self.engine.GWC_KILL_ALL}
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_query_tile_cache_tasks(self, mock_ws, mock_get):
        mock_response = mock.MagicMock(status_code=200)
        mock_ws().name = self.workspace_name
        mock_response.json.return_value = {
            "long-array-array": [[1, 100, 99, 1, 1], [10, 100, 90, 2, -2]]
        }
        mock_get.return_value = mock_response
        layer_id = "gwc_layer_name"
        ret = self.engine.query_tile_cache_tasks(layer_id)

        url = "{endpoint}seed/{workspace}:{name}.json".format(
            endpoint=self.engine.get_gwc_endpoint(),
            workspace=self.workspace_name,
            name="gwc_layer_name",
        )

        # Create feature type call
        mock_get.assert_called_with(url, auth=self.auth)

        self.assertIsInstance(ret, list)
        self.assertEqual(2, len(ret))
        self.assertEqual(
            {
                "tiles_processed": 1,
                "total_to_process": 100,
                "num_remaining": 99,
                "task_id": 1,
                "task_status": "Running",
            },
            ret[0],
        )
        self.assertEqual(
            {
                "tiles_processed": 10,
                "total_to_process": 100,
                "num_remaining": 90,
                "task_id": 2,
                "task_status": -2,
            },
            ret[1],
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    def test_query_tile_cache_tasks_exception(self, mock_get):
        mock_response = mock.MagicMock(status_code=500)
        mock_get.return_value = mock_response
        layer_id = f"{self.workspace_name}:gwc_layer_name"
        self.assertRaises(
            requests.RequestException, self.engine.query_tile_cache_tasks, layer_id
        )

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_coverage_store(self, mock_post, _):
        mock_post.return_value = MockResponse(201)
        store_id = f"{self.workspace_name}:foo"
        coverage_type = "ArcGrid"
        self.engine.create_coverage_store(store_id, coverage_type)
        mock_post.assert_called()
        post_call_args = mock_post.call_args_list
        url = "workspaces/{workspace}/coveragestores".format(
            workspace=self.workspace_name
        )
        self.assertIn(url, post_call_args[0][1]["url"])
        self.assertIn("foo", post_call_args[0][1]["data"])
        self.assertIn(coverage_type, post_call_args[0][1]["data"])

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_create_coverage_store_grass_grid(self, mock_workspace, mock_post, _):
        mock_post.return_value = MockResponse(201)
        mock_workspace().name = self.workspace_name
        store_id = "foo"
        coverage_type = "GrassGrid"  # function converts this to ArcGrid
        self.engine.create_coverage_store(store_id, coverage_type)
        mock_post.assert_called()
        post_call_args = mock_post.call_args_list
        url = "workspaces/{workspace}/coveragestores".format(
            workspace=self.workspace_name
        )
        self.assertIn(url, post_call_args[0][1]["url"])
        self.assertIn("foo", post_call_args[0][1]["data"])
        self.assertIn("ArcGrid", post_call_args[0][1]["data"])
        self.assertNotIn(coverage_type, post_call_args[0][1]["data"])

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_coverage_store_exception(self, mock_post, _):
        mock_post.return_value = MockResponse(500)
        store_id = f"{self.workspace_name}:foo"
        coverage_type = "ArcGrid"
        self.assertRaises(
            requests.RequestException,
            self.engine.create_coverage_store,
            store_id,
            coverage_type,
        )

    def test_create_coverage_store_invalid_type(self):
        store_id = f"{self.workspace_name}:foo"
        coverage_type = "INVALID_COVERAGE_TYPE"
        self.assertRaises(
            ValueError, self.engine.create_coverage_store, store_id, coverage_type
        )

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_create_coverage_layer(self, mock_workspace, mock_put, mock_get_layer, _):
        coverage_name = "adem"
        expected_store_id = (
            coverage_name  # layer and store share name (one to one approach)
        )
        mock_workspace.return_value = self.mock_workspaces[0]
        expected_coverage_type = "GeoTIFF"
        coverage_file_name = "adem.tif"
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        mock_layer_dict = {
            "success": True,
            "result": {"name": coverage_name, "workspace": self.workspace_names[0]},
        }
        mock_get_layer.return_value = mock_layer_dict
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_layer(
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            default_style="points",
            debug=False,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r["name"])
        self.assertEqual(self.workspace_names[0], r["workspace"])

        mock_get_layer.assert_called_with(coverage_name, expected_store_id, False)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = "{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}".format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=expected_store_id,
            ext=expected_coverage_type.lower(),
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml",
        }
        expected_params = {"coverageName": coverage_name}
        self.assertEqual(expected_url, put_call_args[0][1]["url"])
        self.assertEqual(expected_headers, put_call_args[0][1]["headers"])
        self.assertEqual(expected_params, put_call_args[0][1]["params"])

    def test_create_coverage_layer_invalid_coverage_type(self):
        coverage_name = "{}:adem".format(self.workspace_names[0])
        expected_coverage_type = "test1"
        coverage_file_name = "adem.tif"
        coverage_file = os.path.join(self.files_root, coverage_file_name)

        # Raise ValueError
        self.assertRaises(
            ValueError,
            self.engine.create_coverage_layer,
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            debug=False,
        )

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_coverage_layer_zip_file(self, mock_put, mock_get_layer):
        coverage_name = "{}:precip30min".format(self.workspace_names[0])
        expected_store_id = (
            "precip30min"  # layer and store share name (one to one approach)
        )
        expected_coverage_type = "ArcGrid"
        coverage_file_name = "precip30min.zip"
        coverage_file = os.path.join(self.files_root, "arc_sample", coverage_file_name)

        mock_layer_dict = {
            "success": True,
            "result": {"name": coverage_name, "workspace": self.workspace_names[0]},
        }
        mock_get_layer.return_value = mock_layer_dict
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_layer(
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            debug=False,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r["name"])
        self.assertEqual(self.workspace_names[0], r["workspace"])

        mock_get_layer.assert_called_with(coverage_name, expected_store_id, False)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = "{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}".format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=expected_store_id,
            ext=expected_coverage_type.lower(),
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml",
        }
        expected_params = {"coverageName": "precip30min"}
        self.assertEqual(expected_url, put_call_args[0][1]["url"])
        self.assertEqual(expected_headers, put_call_args[0][1]["headers"])
        self.assertEqual(expected_params, put_call_args[0][1]["params"])

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_coverage_layer_grass_grid(self, mock_put, mock_get_layer):
        coverage_name = "{}:my_grass".format(self.workspace_names[0])
        expected_store_id = "my_grass"
        expected_coverage_type = "GrassGrid"
        coverage_file_name = "my_grass.zip"
        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        mock_layer_dict = {
            "success": True,
            "result": {"name": coverage_name, "workspace": self.workspace_names[0]},
        }
        mock_get_layer.return_value = mock_layer_dict
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_layer(
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            debug=False,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r["name"])
        self.assertEqual(self.workspace_names[0], r["workspace"])

        mock_get_layer.assert_called_with(coverage_name, expected_store_id, False)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = "{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}".format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=expected_store_id,
            ext="arcgrid",
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml",
        }
        expected_params = {"coverageName": "my_grass"}
        self.assertEqual(expected_url, put_call_args[0][1]["url"])
        self.assertEqual(expected_headers, put_call_args[0][1]["headers"])
        self.assertEqual(expected_params, put_call_args[0][1]["params"])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.os.path.isdir")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.os.listdir")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_coverage_layer_grass_grid_skip_dir(
        self, mock_put, mock_get_layer, mock_contents, mock_isdir
    ):
        coverage_name = "{}:my_grass".format(self.workspace_names[0])
        expected_store_id = "my_grass"
        expected_coverage_type = "GrassGrid"
        coverage_file_name = "my_grass.zip"
        mock_isdir.side_effect = [True, False]
        mock_contents.side_effect = [
            ["file1", coverage_file_name.replace(".zip", ".asc")],
            [
                coverage_file_name.replace(".zip", ".prj"),
                coverage_file_name.replace(".zip", ".asc"),
            ],
        ]

        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        mock_layer_dict = {
            "success": True,
            "result": {"name": coverage_name, "workspace": self.workspace_names[0]},
        }
        mock_get_layer.return_value = mock_layer_dict
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_layer(
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            debug=False,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r["name"])
        self.assertEqual(self.workspace_names[0], r["workspace"])

        mock_get_layer.assert_called_with(coverage_name, expected_store_id, False)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = "{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}".format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=expected_store_id,
            ext="arcgrid",
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml",
        }
        expected_params = {"coverageName": "my_grass"}
        self.assertEqual(expected_url, put_call_args[0][1]["url"])
        self.assertEqual(expected_headers, put_call_args[0][1]["headers"])
        self.assertEqual(expected_params, put_call_args[0][1]["params"])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.os.listdir")
    def test_create_coverage_layer_grass_grid_exception(
        self, mock_working_dir_contents
    ):
        coverage_name = "{}:my_grass".format(self.workspace_names[0])
        expected_coverage_type = "GrassGrid"
        coverage_file_name = "my_grass.zip"
        mock_working_dir_contents.return_value = [coverage_file_name, "file2", "file3"]
        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        # Raise ValueError
        self.assertRaises(
            ValueError,
            self.engine.create_coverage_layer,
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            debug=False,
        )

    def test_create_coverage_layer_grass_invalid_file(self):
        coverage_name = "{}:my_grass".format(self.workspace_names[0])
        expected_coverage_type = "GrassGrid"
        coverage_file_name = "my_grass_invalid.zip"
        coverage_file = os.path.join(self.files_root, "grass_ascii", coverage_file_name)

        # Execute
        self.assertRaises(
            IOError,
            self.engine.create_coverage_layer,
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            debug=False,
        )

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_coverage_layer_image_mosaic(self, mock_put, mock_get_layer):
        coverage_name = "{}:global_mosaic".format(self.workspace_names[0])
        expected_store_id = (
            "global_mosaic"  # layer and store share name (one to one approach)
        )
        expected_coverage_type = "ImageMosaic"
        coverage_file_name = "global_mosaic.zip"
        coverage_file = os.path.join(
            self.files_root, "mosaic_sample", coverage_file_name
        )

        mock_layer_dict = {
            "success": True,
            "result": {"name": coverage_name, "workspace": self.workspace_names[0]},
        }
        mock_get_layer.return_value = mock_layer_dict
        mock_put.return_value = MockResponse(201)

        # Execute
        response = self.engine.create_coverage_layer(
            layer_id=coverage_name,
            coverage_type=expected_coverage_type,
            coverage_file=coverage_file,
            debug=False,
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        # Values
        self.assertEqual(coverage_name, r["name"])
        self.assertEqual(self.workspace_names[0], r["workspace"])

        mock_get_layer.assert_called_with(coverage_name, expected_store_id, False)

        # PUT Tests
        put_call_args = mock_put.call_args_list
        expected_url = "{endpoint}workspaces/{w}/coveragestores/{s}/file.{ext}".format(
            endpoint=self.endpoint,
            w=self.workspace_names[0],
            s=expected_store_id,
            ext=expected_coverage_type.lower(),
        )
        expected_headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml",
        }

        self.assertEqual(expected_url, put_call_args[0][1]["url"])
        self.assertEqual(expected_headers, put_call_args[0][1]["headers"])

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_coverage_layer_already_exists(
        self, mock_put, mock_log, mock_get_layer
    ):
        mock_put.return_value = MockResponse(500, "already exists")
        coverage_name = f"{self.workspace_name}:foo"
        coverage_type = "ArcGrid"
        coverage_file = os.path.join(self.files_root, "arc_sample", "precip30min.asc")
        self.engine.create_coverage_layer(
            layer_id=coverage_name,
            coverage_type=coverage_type,
            coverage_file=coverage_file,
        )
        mock_put.assert_called()
        put_call_args = mock_put.call_args_list
        url = "workspaces/{workspace}/coveragestores/{coverage_store_name}/file.{extension}".format(
            workspace=self.workspace_name,
            coverage_store_name="foo",
            extension=coverage_type.lower(),
        )
        self.assertIn(url, put_call_args[0][1]["url"])
        self.assertIn("coverageName", put_call_args[0][1]["params"])
        self.assertEqual("foo", put_call_args[0][1]["params"]["coverageName"])
        self.assertIn("files", put_call_args[0][1])
        mock_log.warning.assert_called()
        mock_get_layer.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_coverage_layer_error_unzipping(self, mock_put, mock_log):
        mock_put.return_value = MockResponse(500, "Error occured unzipping file")
        coverage_name = f"{self.workspace_name}:foo"
        coverage_type = "ArcGrid"
        coverage_file = os.path.join(self.files_root, "arc_sample", "precip30min.asc")
        self.assertRaises(
            requests.RequestException,
            self.engine.create_coverage_layer,
            layer_id=coverage_name,
            coverage_type=coverage_type,
            coverage_file=coverage_file,
        )
        num_put_calls = len(mock_put.call_args_list)
        self.assertEqual(5, num_put_calls)
        mock_log.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_coverage_layer_error(self, mock_put, mock_log):
        mock_put.return_value = MockResponse(500, "500 exception")
        coverage_name = f"{self.workspace_name}:foo"
        coverage_type = "ArcGrid"
        coverage_file = os.path.join(self.files_root, "arc_sample", "precip30min.asc")
        self.assertRaises(
            requests.RequestException,
            self.engine.create_coverage_layer,
            layer_id=coverage_name,
            coverage_type=coverage_type,
            coverage_file=coverage_file,
        )
        num_put_calls = len(mock_put.call_args_list)
        self.assertEqual(3, num_put_calls)
        mock_log.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_enable_time_dimension(self, mock_ws, mock_put, _):
        mock_response = mock.MagicMock(status_code=200)
        mock_ws().name = self.workspace_name
        mock_put.return_value = mock_response
        coverage_id = "foo"
        self.engine.enable_time_dimension(coverage_id=coverage_id)
        mock_put.assert_called()
        put_call_args = mock_put.call_args_list
        url = "{endpoint}workspaces/{workspace}/coveragestores/{coverage_name}/coverages/{coverage_name}".format(
            endpoint=self.endpoint,
            workspace=self.workspace_name,
            coverage_name=coverage_id,
        )
        self.assertEqual(url, put_call_args[0][0][0])
        self.assertIn("data", put_call_args[0][1])

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_enable_time_dimension_exception(self, mock_put, mock_log):
        mock_response = mock.MagicMock(status_code=500)
        mock_put.return_value = mock_response
        coverage_id = f"{self.workspace_name}:foo"
        self.assertRaises(
            requests.RequestException, self.engine.enable_time_dimension, coverage_id
        )

        url = "{endpoint}workspaces/{workspace}/coveragestores/{coverage_name}/coverages/{coverage_name}".format(
            endpoint=self.endpoint,
            workspace=self.workspace_name,
            coverage_name="foo",
        )

        put_call_args = mock_put.call_args_list
        self.assertEqual(url, put_call_args[0][0][0])
        self.assertIn("data", put_call_args[0][1])

        mock_log.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_workspace(self, mock_catalog):
        mc = mock_catalog()
        expected_uri = "http:www.example.com/b-workspace"

        mc.create_workspace.return_value = self.mock_workspaces[0]

        # Execute
        response = self.engine.create_workspace(
            workspace_id=self.workspace_names[0], uri=expected_uri
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Type
        self.assertIsInstance(r, dict)

        self.assertIn("name", r)
        self.assertEqual(self.workspace_names[0], r["name"])

        mc.create_workspace.assert_called_with(self.workspace_names[0], expected_uri)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_create_workspace_assertion_error(self, mock_catalog):
        mc = mock_catalog()
        expected_uri = "http:www.example.com/b-workspace"
        mc.create_workspace.side_effect = AssertionError("AssertionError")

        # Execute
        response = self.engine.create_workspace(
            workspace_id=self.workspace_names[0], uri=expected_uri
        )
        # False
        self.assertFalse(response["success"])
        # Expect Error
        r = response["error"]
        # Properties
        self.assertIn("AssertionError", r)
        mc.create_workspace.assert_called_with(self.workspace_names[0], expected_uri)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_style"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_create_style(self, mock_workspace, mock_post, mock_get_style, mock_log):
        mock_post.return_value = mock.MagicMock(status_code=201)
        mock_workspace.return_value = self.mock_workspaces[0]
        style_id = "{}:{}".format(
            self.mock_workspaces[0].name, self.mock_styles[0].name
        )
        sld_template = os.path.join(self.files_root, "test_create_style.sld")
        sld_context = {"foo": "bar"}

        mock_get_style.return_value = {
            'success': True,
            'result': {'name': self.mock_styles[0].name, 'workspace': self.workspace_name}
        }

        response = self.engine.create_style(style_id, sld_template, sld_context)

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        # Values
        self.assertEqual(self.mock_styles[0].name, r["name"])
        self.assertEqual(self.workspace_name, r["workspace"])

        # Validate endpoint calls
        style_url = "workspaces/{w}/styles".format(w=self.mock_workspaces[0].name)

        # Create feature type call
        post_call_args = mock_post.call_args_list
        self.assertIn(style_url, post_call_args[0][0][0])
        mock_log.info.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_style"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_style_cannot_find_style(self, mock_post, mock_get_style, mock_log):
        mock_post.return_value = mock.MagicMock(
            status_code=500, text="Unable to find style for event"
        )
        style_name = self.mock_styles[0].name
        sld_template = os.path.join(self.files_root, "test_create_style.sld")
        sld_context = {"foo": "bar"}

        mock_get_style.return_value = {
            'success': True,
            'result': ' warnings '
        }

        self.engine.create_style(style_name, sld_template, sld_context)

        # Validate endpoint calls
        style_url = "{endpoint}styles".format(endpoint=self.endpoint)

        # Create feature type call
        post_call_args = mock_post.call_args_list
        self.assertIn(style_url, post_call_args[0][0][0])
        mock_log.warning.assert_called()

    @mock.patch('tethys_dataset_services.engines.geoserver_engine.log')
    @mock.patch('tethys_dataset_services.engines.geoserver_engine.requests.post')
    def test_create_style_exception(self, mock_post, mock_log):
        mock_post.return_value = mock.MagicMock(status_code=500, text="500 exception")
        style_name = self.mock_styles[0].name
        sld_template = os.path.join(self.files_root, "test_create_style.sld")
        sld_context = {"foo": "bar"}

        self.assertRaises(
            requests.RequestException,
            self.engine.create_style,
            style_name,
            sld_template,
            sld_context,
        )
        mock_log.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_style_other_exception(self, mock_post, mock_log):
        mock_post.return_value = mock.MagicMock(status_code=504, text="504 exception")
        style_name = self.mock_styles[0].name
        sld_template = os.path.join(self.files_root, "test_create_style.sld")
        sld_context = {"foo": "bar"}

        with self.assertRaises(requests.RequestException) as context:
            self.engine.create_style(style_name, sld_template, sld_context)
        self.assertEqual(
            "Create Style Status Code 504: 504 exception", str(context.exception)
        )
        mock_log.error.assert_called()

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_style"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_style_overwrite(self, mock_post, mock_logger, mock_get_style):
        """
        Attempt to delete resulting in no style found is OK,
        so should proceed to create style.
        """
        mock_post.return_value = mock.MagicMock(status_code=201)
        self.delete_style = mock.MagicMock(side_effect=Exception("no such style"))
        style_id = f"{self.workspace_name}:{self.mock_styles[0].name}"
        sld_template = os.path.join(self.files_root, "test_create_style.sld")
        sld_context = {"foo": "bar"}
        self.engine.delete_style = mock.MagicMock()
        mock_get_style.return_value = {
            'success': True,
            'result': {'name': self.mock_styles[0].name, 'workspace': self.workspace_name}
        }

        # Execute
        response = self.engine.create_style(
            style_id, sld_template, sld_context, overwrite=True
        )

        # Validate response object
        self.assert_valid_response_object(response)

        # Success
        self.assertTrue(response['success'])

        # Extract Result
        result = response["result"]

        # Type
        self.assertIsInstance(result, dict)

        # Overwrite
        self.engine.delete_style.assert_called_with(style_id, purge=True)

        # Validate endpoint calls
        style_url = f"{self.endpoint}workspaces/{self.workspace_name}/styles"
        mock_post.assert_called_with(
            style_url,
            headers={"Content-type": "application/vnd.ogc.sld+xml"},
            auth=self.auth,
            params={"name": self.mock_styles[0].name},
            data=mock.ANY,
        )

        # Validate SLD was rendered correctly
        rendered_sld_path = os.path.join(
            self.files_root, "test_create_style_rendered.sld"
        )
        with open(rendered_sld_path) as rendered:
            rendered_sld = rendered.read()
        self.assertEqual(rendered_sld, mock_post.call_args_list[0][1]["data"])

        # Verify log messages
        mock_logger.info.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    def test_create_style_overwrite_referenced_by_existing(self, mock_logger):
        style_id = f"{self.workspace_name}:{self.mock_styles[0].name}"
        sld_template = os.path.join(self.files_root, "test_create_style.sld")
        sld_context = {"foo": "bar"}
        self.engine.delete_style = mock.MagicMock(
            side_effect=ValueError("referenced by existing")
        )

        # Execute
        with self.assertRaises(ValueError) as error:
            self.engine.create_style(
                style_id, sld_template, sld_context, overwrite=True
            )

        self.assertEqual("referenced by existing", str(error.exception))

        mock_logger.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_sql_view_layer(
        self,
        mock_post,
        mock_logger,
        mock_update_layer_styles,
        mock_get_layer,
        mock_reload,
        mock_put
    ):
        mock_post.side_effect = [MockResponse(201)]  # featuretype create
        mock_put.return_value = MockResponse(200)    # GWC layer create
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        geometry_type = "Point"
        srid = 4236
        sql = "SELECT * FROM foo"
        default_style = "points"

        self.engine.create_sql_view_layer(
            store_id, layer_name, geometry_type, srid, sql, default_style,
            gwc_method="PUT"  # force create path; uses mock_put and avoids extra POST
        )

        # Validate endpoint calls
        sql_view_url = (
            "workspaces/{workspace}/datastores/{datastore}/featuretypes".format(
                workspace=self.workspace_name, datastore="foo"
            )
        )
        gwc_layer_url = "layers/{workspace}:{feature_name}.xml".format(
            workspace=self.workspace_name, feature_name=layer_name
        )

        with open(
            os.path.join(self.files_root, "test_create_layer_sql_view.xml")
        ) as rendered:
            expected_sql_xml = rendered.read()
        with open(
            os.path.join(self.files_root, "test_create_layer_gwc_layer.xml")
        ) as rendered:
            expected_gwc_lyr_xml = rendered.read()

        # Create feature type call
        post_call_args = mock_post.call_args_list
        self.assertIn(sql_view_url, post_call_args[0][0][0])
        self.assertEqual(expected_sql_xml, post_call_args[0][1]["data"])

        put_call_args = mock_put.call_args_list
        self.assertIn(gwc_layer_url, put_call_args[0][0][0])
        self.assertEqual(expected_gwc_lyr_xml, str(put_call_args[0][1]["data"]))

        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_get_layer.assert_called()
        mock_reload.assert_called()

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_create_layer_create_feature_type_already_exists(
        self,
        mock_workspace,
        mock_put,
        mock_post,
        mock_logger,
        mock_update_layer_styles,
        mock_get_layer,
        mock_reload,
    ):
        mock_post.side_effect = [MockResponse(500, "already exists"), MockResponse(200)]
        mock_put.return_value = MockResponse(200)  # ensure no accidental failure if called
        mock_workspace().name = self.workspace_name
        store_id = 'foo'
        layer_name = self.layer_names[0]
        geometry_type = 'Point'
        srid = 4236
        sql = "SELECT * FROM foo"
        default_style = "points"

        self.engine.create_sql_view_layer(
            store_id, layer_name, geometry_type, srid, sql, default_style,
            gwc_method="POST"
        )

        # Validate endpoint calls
        sql_view_url = (
            "workspaces/{workspace}/datastores/{datastore}/featuretypes".format(
                workspace=self.workspace_name, datastore="foo"
            )
        )
        gwc_layer_url = "layers/{workspace}:{feature_name}.xml".format(
            workspace=self.workspace_name, feature_name=layer_name
        )

        with open(
            os.path.join(self.files_root, "test_create_layer_sql_view.xml")
        ) as rendered:
            expected_sql_xml = rendered.read()
        with open(
            os.path.join(self.files_root, "test_create_layer_gwc_layer.xml")
        ) as rendered:
            expected_gwc_lyr_xml = rendered.read()

        # Create feature type call
        post_call_args = mock_post.call_args_list
        self.assertIn(sql_view_url, post_call_args[0][0][0])
        self.assertEqual(expected_sql_xml, post_call_args[0][1]["data"])

        # GWC Call
        post_call_args = mock_post.call_args_list
        self.assertIn(gwc_layer_url, post_call_args[1][0][0])
        self.assertEqual(expected_gwc_lyr_xml, str(post_call_args[1][1]["data"]))
        mock_logger.info.assert_called()

        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_get_layer.assert_called()
        mock_reload.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_layer_create_sql_view_exception(self, mock_post, mock_logger):
        mock_post.return_value = MockResponse(500, "other exception")
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        geometry_type = "Point"
        srid = 4236
        sql = "SELECT * FROM foo"
        default_style = "points"

        with self.assertRaises(requests.RequestException) as error:
            self.engine.create_sql_view_layer(
                store_id, layer_name, geometry_type, srid, sql, default_style
            )

        self.assertEqual(
            "Create Feature Type Status Code 500: other exception", str(error.exception)
        )
        mock_logger.error.assert_called()

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    def test_create_sql_view_layer_gwc_error(self, mock_put, mock_logger, mock_post, _):
        mock_post.side_effect = [MockResponse(201), MockResponse(200)]
        mock_put.return_value = MockResponse(500, "GWC exception")
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        geometry_type = "Point"
        srid = 4236
        sql = "SELECT * FROM foo"
        default_style = "points"

        with self.assertRaises(requests.RequestException) as error:
            self.engine.create_sql_view_layer(
                store_id, layer_name, geometry_type, srid, sql, default_style,
                gwc_method="PUT"
            )

        self.assertEqual(
            "Create/Update GWC Layer Status Code 500: GWC exception", str(error.exception)
        )
        mock_logger.error.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_sql_view_layer_gwc_invalid_method(
        self, mock_post, mock_put, mock_get, mock_update_layer_styles, mock_reload
    ):
        # Feature type creation succeeds, then invalid gwc_method triggers ValueError
        mock_post.return_value = MockResponse(201)
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        default_style = "points"

        with self.assertRaises(ValueError) as err:
            self.engine.create_sql_view_layer(
                store_id, layer_name, "Point", 4236, "SELECT * FROM foo", "points",
                gwc_method="BAD"
            )
        self.assertIn("gwc_method must be one of 'AUTO', 'POST', or 'PUT'", str(err.exception))
        # ensure we didn't try to probe or call GWC after the check
        mock_get.assert_not_called()
        mock_put.assert_not_called()
        # FT creation happened
        mock_post.assert_called_once()
        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_reload.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_sql_view_layer_gwc_auto_probe_exists_uses_post(
        self, mock_post, mock_put, mock_get, mock_get_layer, mock_update_layer_styles, mock_reload
    ):
        # FT create (POST 201), then AUTO probe (GET 200) -> GWC POST (modify 200)
        mock_post.side_effect = [MockResponse(201), MockResponse(200)]
        mock_get.return_value = MockResponse(200)
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        default_style = "points"

        self.engine.create_sql_view_layer(
            store_id, layer_name, "Point", 4236, "SELECT * FROM foo", "points",
            gwc_method="AUTO"
        )

        gwc_layer_url = "layers/{workspace}:{feature_name}.xml".format(
            workspace=self.workspace_name, feature_name=layer_name
        )
        # second POST call should be to GWC
        post_calls = mock_post.call_args_list
        self.assertIn(gwc_layer_url, post_calls[1][0][0])
        mock_put.assert_not_called()
        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_get_layer.assert_called()
        mock_reload.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_sql_view_layer_gwc_auto_probe_missing_uses_put(
        self, mock_post, mock_put, mock_get, mock_get_layer, mock_update_layer_styles, mock_reload
    ):
        # FT create (POST 201), then AUTO probe (GET 404) -> GWC PUT (create 200)
        mock_post.side_effect = [MockResponse(201)]
        mock_put.return_value = MockResponse(200)
        mock_get.return_value = MockResponse(404)
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        default_style = "points"

        self.engine.create_sql_view_layer(
            store_id, layer_name, "Point", 4236, "SELECT * FROM foo", "points",
            gwc_method="AUTO"
        )

        gwc_layer_url = "layers/{workspace}:{feature_name}.xml".format(
            workspace=self.workspace_name, feature_name=layer_name
        )
        put_calls = mock_put.call_args_list
        self.assertIn(gwc_layer_url, put_calls[0][0][0])
        # only one POST (feature type), no GWC POST
        self.assertEqual(len(mock_post.call_args_list), 1)

        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_get_layer.assert_called()
        mock_reload.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_sql_view_layer_gwc_auto_probe_unknown_status_uses_post(
        self, mock_post, mock_put, mock_get, mock_get_layer, mock_update_layer_styles, mock_reload
    ):
        # FT create (POST 201), then AUTO probe (GET 500) -> GWC POST (modify 200) as safe default
        mock_post.side_effect = [MockResponse(201), MockResponse(200)]
        mock_get.return_value = MockResponse(500)
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        default_style = "points"

        self.engine.create_sql_view_layer(
            store_id, layer_name, "Point", 4236, "SELECT * FROM foo", "points",
            gwc_method="AUTO"
        )

        gwc_layer_url = "layers/{workspace}:{feature_name}.xml".format(
            workspace=self.workspace_name, feature_name=layer_name
        )
        self.assertIn(gwc_layer_url, mock_post.call_args_list[1][0][0])
        mock_put.assert_not_called()

        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_get_layer.assert_called()
        mock_reload.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.get")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_sql_view_layer_gwc_auto_probe_exception_uses_post(
        self, mock_post, mock_put, mock_get, mock_get_layer, mock_update_layer_styles, mock_reload
    ):
        # FT create (POST 201), then AUTO probe raises -> GWC POST (modify 200)
        mock_post.side_effect = [MockResponse(201), MockResponse(200)]
        mock_get.side_effect = Exception("probe failed")
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        default_style = "points"
        self.engine.create_sql_view_layer(
            store_id, layer_name, "Point", 4236, "SELECT * FROM foo", "points",
            gwc_method="AUTO"
        )

        gwc_layer_url = "layers/{workspace}:{feature_name}.xml".format(
            workspace=self.workspace_name, feature_name=layer_name
        )
        self.assertIn(gwc_layer_url, mock_post.call_args_list[1][0][0])
        mock_put.assert_not_called()
        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_get_layer.assert_called()
        mock_reload.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.reload")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.update_layer_styles")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_layer"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.put")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_sql_view_layer_gwc_put_fallbacks_to_post_when_exists(
        self, mock_post, mock_put, mock_get_layer, mock_update_layer_styles, mock_reload
    ):
        # FT create (POST 201)
        # GWC PUT returns 409 "already exists" -> code falls back to POST next loop -> 200
        mock_post.side_effect = [MockResponse(201), MockResponse(200)]
        mock_put.side_effect = [MockResponse(409, "already exists")]
        store_id = f"{self.workspace_name}:foo"
        layer_name = self.layer_names[0]
        default_style = "points"

        self.engine.create_sql_view_layer(
            store_id, layer_name, "Point", 4236, "SELECT * FROM foo", "points",
            gwc_method="PUT"
        )

        gwc_layer_url = "layers/{workspace}:{feature_name}.xml".format(
            workspace=self.workspace_name, feature_name=layer_name
        )
        # First PUT attempted once
        self.assertIn(gwc_layer_url, mock_put.call_args_list[0][0][0])
        # Then POST used after fallback (second POST call overall)
        self.assertIn(gwc_layer_url, mock_post.call_args_list[1][0][0])

        mock_update_layer_styles.assert_called_with(
            layer_id=f"{self.workspace_name}:{layer_name}",
            default_style=default_style,
            other_styles=None,
        )
        mock_get_layer.assert_called()
        mock_reload.assert_called()

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog")
    def test_apply_changes_to_gs_object(self, mock_catalog):
        mc = mock_catalog()
        gs_object = mock.NonCallableMagicMock(
            layer_id=self.layer_names[0],
            styles=self.style_names,
            default_style="d_styles",
        )
        # new style
        new_gs_args = {
            "styles": ["style1:style1a", "style2"],
            "default_style": "dstyle1",
        }

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
        new_gs_args = {"default_style": "dstyle1: dstyle2"}

        # mock get_style to return value
        mc.get_style.return_value = self.mock_styles[0]

        # Execute
        new_gs_object = self.engine._apply_changes_to_gs_object(new_gs_args, gs_object)

        d_style = new_gs_object.default_style.name

        # validate
        self.assertIn(self.mock_styles[0].name, d_style)

    def test_get_non_rest_endpoint(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint="http://localhost:8181/geoserver/rest/",
        )

        expected_endpoint = "http://localhost:8181/geoserver"
        endpoint = self.engine._get_non_rest_endpoint()

        # Check Response
        self.assertEqual(expected_endpoint, endpoint)

    def test_get_wms_url(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint="http://localhost:8181/geoserver/rest/",
        )

        # tiled and transparent are set as default value
        wms_url = self.engine._get_wms_url(
            layer_id=self.layer_names[0],
            style=self.style_names[0],
            srs="EPSG:4326",
            bbox="-180,-90,180,90",
            version="1.1.0",
            width="512",
            height="512",
            output_format="image/png",
            tiled=False,
            transparent=True,
        )

        expected_url = (
            "http://localhost:8181/geoserver/wms?service=WMS&version=1.1.0&"
            "request=GetMap&layers={0}&styles={1}&transparent=true&"
            "tiled=no&srs=EPSG:4326&bbox=-180,-90,180,90&"
            "width=512&height=512&format=image/png".format(
                self.layer_names[0], self.style_names[0]
            )
        )

        # check wms_url
        self.assertEqual(expected_url, wms_url)

        # tiled and transparent are set as default value
        wms_url = self.engine._get_wms_url(
            layer_id=self.layer_names[0],
            style=self.style_names[0],
            srs="EPSG:4326",
            bbox="-180,-90,180,90",
            version="1.1.0",
            width="512",
            height="512",
            output_format="image/png",
            tiled=True,
            transparent=False,
        )

        expected_url = (
            "http://localhost:8181/geoserver/wms?service=WMS&version=1.1.0&"
            "request=GetMap&layers={0}&styles={1}&transparent=false&"
            "tiled=yes&srs=EPSG:4326&bbox=-180,-90,180,90&"
            "width=512&height=512&format=image/png".format(
                self.layer_names[0], self.style_names[0]
            )
        )

        # check wms_url
        self.assertEqual(expected_url, wms_url)

    def test_get_wcs_url(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint="http://localhost:8181/geoserver/rest/",
        )

        wcs_url = self.engine._get_wcs_url(
            resource_id=self.resource_names[0],
            srs="EPSG:4326",
            bbox="-180,-90,180,90",
            output_format="png",
            namespace=self.store_name,
            width="512",
            height="512",
        )

        expected_wcs_url = (
            "http://localhost:8181/geoserver/wcs?service=WCS&version=1.1.0&"
            "request=GetCoverage&identifier={0}&srs=EPSG:4326&"
            "BoundingBox=-180,-90,180,90&width=512&"
            "height=512&format=png&namespace={1}".format(
                self.resource_names[0], self.store_name
            )
        )

        # check wcs_url
        self.assertEqual(expected_wcs_url, wcs_url)

    def test_get_wfs_url(self):
        self.engine = GeoServerSpatialDatasetEngine(
            endpoint="http://localhost:8181/geoserver/rest/",
        )

        # GML3 Case
        wfs_url = self.engine._get_wfs_url(
            resource_id=self.resource_names[0], output_format="GML3"
        )
        expected_wfs_url = (
            "http://localhost:8181/geoserver/wfs?service=WFS&"
            "version=2.0.0&request=GetFeature&"
            "typeNames={0}".format(self.resource_names[0])
        )
        # check wcs_url
        self.assertEqual(expected_wfs_url, wfs_url)

        # GML2 Case
        wfs_url = self.engine._get_wfs_url(
            resource_id=self.resource_names[0], output_format="GML2"
        )
        expected_wfs_url = (
            "http://localhost:8181/geoserver/wfs?service=WFS&"
            "version=1.0.0&request=GetFeature&"
            "typeNames={0}&outputFormat={1}".format(self.resource_names[0], "GML2")
        )
        # check wcs_url
        self.assertEqual(expected_wfs_url, wfs_url)

        # Other format Case
        wfs_url = self.engine._get_wfs_url(
            resource_id=self.resource_names[0], output_format="Other"
        )
        expected_wfs_url = (
            "http://localhost:8181/geoserver/wfs?service=WFS&"
            "version=2.0.0&request=GetFeature&"
            "typeNames={0}&outputFormat={1}".format(self.resource_names[0], "Other")
        )
        # check wcs_url
        self.assertEqual(expected_wfs_url, wfs_url)

    @mock.patch("sys.stdout", new_callable=StringIO)
    def test_handle_debug(self, mock_print):
        test_object = self.style_names

        self.engine._handle_debug(test_object, debug=True)

        output = mock_print.getvalue()

        # check results
        self.assertIn(self.style_names[0], output)

    def test_transcribe_geoserver_object(self):

        # NAMED_OBJECTS
        gs_object_store = mock.NonCallableMagicMock(
            store=self.store_name, styles=self.style_names
        )
        store_dict = self.engine._transcribe_geoserver_object(gs_object_store)

        # check if type is dic
        self.assertIsInstance(store_dict, dict)

        # check properties
        self.assertIn(self.store_name, store_dict["store"])
        self.assertIn(self.style_names[0], store_dict["styles"])

        # NAMED_OBJECTS_WITH_WORKSPACE
        gs_sub_object_resource = mock.NonCallableMagicMock(
            workspace=self.workspace_name, writers="test_omit_attributes"
        )
        gs_sub_object_resource.name = self.resource_names[0]
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            default_style=self.default_style_name,
        )
        resource_dict = self.engine._transcribe_geoserver_object(gs_object_resource)

        # check if type is dic
        self.assertIsInstance(resource_dict, dict)

        # check properties
        resource_att = "{0}:{1}".format(self.workspace_name, self.resource_names[0])
        self.assertIn(resource_att, resource_dict["resource"])
        self.assertIn(self.default_style_name, resource_dict["default_style"])

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
        self.assertIn(resource_att, resource_dict["resource"])
        self.assertIn(self.default_style_name, resource_dict["default_style"])

        # resource_type with workspace
        gs_object_resource = mock.NonCallableMagicMock(
            resource_type="featureType",
            workspace=self.workspace_name,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(
            gs_object_resource
        )

        self.assertIn("gml3", resource_type_dict["wfs"])

        # resource_type with no workspace
        gs_object_resource = mock.NonCallableMagicMock(
            resource_type="featureType",
            workspace=None,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(
            gs_object_resource
        )

        self.assertIn("gml3", resource_type_dict["wfs"])

        # resource_type with no workspace and coverage
        gs_sub_object_resource = mock.NonCallableMagicMock(
            native_bbox=["0", "1", "2", "3"]
        )
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            resource_type="coverage",
            workspace=None,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(
            gs_object_resource
        )

        self.assertIn("png", resource_type_dict["wcs"])

        # resource_type with workspace and coverage -wcs
        gs_sub_object_resource = mock.NonCallableMagicMock(
            native_bbox=["0", "1", "2", "3"]
        )
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            resource_type="coverage",
            workspace=self.workspace_name,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(
            gs_object_resource
        )

        self.assertIn("png", resource_type_dict["wcs"])

        # resource_type with workspace and layer - wms
        gs_sub_object_resource = mock.NonCallableMagicMock(
            native_bbox=["0", "1", "2", "3"]
        )
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            resource_type="layer",
            workspace=self.workspace_name,
            default_style=self.default_style_name,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(
            gs_object_resource
        )

        self.assertIn("png", resource_type_dict["wms"])

        # resource_type with workspace and layer - wms with bounds
        gs_sub_object_resource = mock.NonCallableMagicMock(
            native_bbox=["0", "1", "2", "3"]
        )
        gs_object_resource = mock.NonCallableMagicMock(
            resource=gs_sub_object_resource,
            bounds=["0", "1", "2", "3", "4"],
            resource_type="layerGroup",
            workspace=self.workspace_name,
            default_style=self.default_style_name,
        )
        gs_object_resource.name = "test_name"
        resource_type_dict = self.engine._transcribe_geoserver_object(
            gs_object_resource
        )

        self.assertIn("png", resource_type_dict["wms"])

    def test_link_sqlalchemy_db_to_geoserver(self):
        self.engine.create_postgis_store = mock.MagicMock()
        url = "postgresql://user:pass@localhost:5432/foo"
        engine = create_engine(url)
        self.engine.link_sqlalchemy_db_to_geoserver(
            store_id=self.store_names[0], sqlalchemy_engine=engine, docker=True
        )
        self.engine.create_postgis_store.assert_called_with(
            store_id=self.store_names[0],
            host="172.17.0.1",
            port=5432,
            database="foo",
            username="user",
            password="pass",
            max_connections=5,
            max_connection_idle_time=30,
            evictor_run_periodicity=30,
            validate_connections=True,
            debug=False,
        )

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_postgis_store_validate_connection(self, mock_post, _):
        mock_post.return_value = MockResponse(201)
        store_id = "{}:foo".format(self.workspace_name)
        host = "localhost"
        port = "5432"
        database = "foo_db"
        username = "user"
        password = "pass"
        max_connections = 10
        max_connection_idle_time = 40
        evictor_run_periodicity = 60

        xml = """
              <dataStore>
                <name>{0}</name>
                <connectionParameters>
                  <entry key="host">{1}</entry>
                  <entry key="port">{2}</entry>
                  <entry key="database">{3}</entry>
                  <entry key="user">{4}</entry>
                  <entry key="passwd">{5}</entry>
                  <entry key="dbtype">postgis</entry>
                  <entry key="max connections">{6}</entry>
                  <entry key="Max connection idle time">{7}</entry>
                  <entry key="Evictor run periodicity">{8}</entry>
                  <entry key="validate connections">true</entry>
                  <entry key="Expose primary keys">false</entry>
                </connectionParameters>
              </dataStore>
              """.format(
            "foo",
            host,
            port,
            database,
            username,
            password,
            max_connections,
            max_connection_idle_time,
            evictor_run_periodicity,
        )

        expected_headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores".format(
            endpoint=self.endpoint, workspace=self.workspace_name
        )
        self.engine.create_postgis_store(
            store_id,
            host,
            port,
            database,
            username,
            password,
            max_connections,
            max_connection_idle_time,
            evictor_run_periodicity,
        )
        mock_post.assert_called_with(
            url=rest_endpoint, data=xml, headers=expected_headers, auth=self.auth
        )

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_create_postgis_store_validate_connection_false(
        self, mock_workspace, mock_post, _
    ):
        mock_post.return_value = MockResponse(201)
        store_id = "foo"
        mock_workspace().name = self.workspace_name
        host = "localhost"
        port = "5432"
        database = "foo_db"
        username = "user"
        password = "pass"
        max_connections = 10
        max_connection_idle_time = 40
        evictor_run_periodicity = 60

        xml = """
              <dataStore>
                <name>{0}</name>
                <connectionParameters>
                  <entry key="host">{1}</entry>
                  <entry key="port">{2}</entry>
                  <entry key="database">{3}</entry>
                  <entry key="user">{4}</entry>
                  <entry key="passwd">{5}</entry>
                  <entry key="dbtype">postgis</entry>
                  <entry key="max connections">{6}</entry>
                  <entry key="Max connection idle time">{7}</entry>
                  <entry key="Evictor run periodicity">{8}</entry>
                  <entry key="validate connections">false</entry>
                  <entry key="Expose primary keys">false</entry>
                </connectionParameters>
              </dataStore>
              """.format(
            "foo",
            host,
            port,
            database,
            username,
            password,
            max_connections,
            max_connection_idle_time,
            evictor_run_periodicity,
        )

        expected_headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores".format(
            endpoint=self.endpoint, workspace=self.workspace_name
        )
        self.engine.create_postgis_store(store_id, host, port, database, username, password, max_connections,
                                         max_connection_idle_time, evictor_run_periodicity, validate_connections=False)
        mock_post.assert_called_with(url=rest_endpoint, data=xml, headers=expected_headers, auth=self.auth)

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    def test_create_postgis_store_expose_primary_keys_true(
        self, mock_workspace, mock_post, _
    ):
        mock_post.return_value = MockResponse(201)
        store_id = "foo"
        mock_workspace().name = self.workspace_name
        host = "localhost"
        port = "5432"
        database = "foo_db"
        username = "user"
        password = "pass"
        max_connections = 10
        max_connection_idle_time = 40
        evictor_run_periodicity = 60

        xml = """
              <dataStore>
                <name>{0}</name>
                <connectionParameters>
                  <entry key="host">{1}</entry>
                  <entry key="port">{2}</entry>
                  <entry key="database">{3}</entry>
                  <entry key="user">{4}</entry>
                  <entry key="passwd">{5}</entry>
                  <entry key="dbtype">postgis</entry>
                  <entry key="max connections">{6}</entry>
                  <entry key="Max connection idle time">{7}</entry>
                  <entry key="Evictor run periodicity">{8}</entry>
                  <entry key="validate connections">false</entry>
                  <entry key="Expose primary keys">true</entry>
                </connectionParameters>
              </dataStore>
              """.format(
            "foo",
            host,
            port,
            database,
            username,
            password,
            max_connections,
            max_connection_idle_time,
            evictor_run_periodicity,
        )

        expected_headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores".format(
            endpoint=self.endpoint, workspace=self.workspace_name
        )
        self.engine.create_postgis_store(store_id, host, port, database, username, password, max_connections,
                                         max_connection_idle_time, evictor_run_periodicity, validate_connections=False,
                                         expose_primary_keys=True)
        mock_post.assert_called_with(url=rest_endpoint, data=xml, headers=expected_headers, auth=self.auth)

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.log")
    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    def test_create_postgis_store_not_201(self, mock_post, mock_logger, _):
        mock_post.return_value = MockResponse(500)
        store_id = "{}:foo".format(self.workspace_name)
        host = "localhost"
        port = "5432"
        database = "foo_db"
        username = "user"
        password = "pass"
        max_connections = 10
        max_connection_idle_time = 40
        evictor_run_periodicity = 60

        xml = """
              <dataStore>
                <name>{0}</name>
                <connectionParameters>
                  <entry key="host">{1}</entry>
                  <entry key="port">{2}</entry>
                  <entry key="database">{3}</entry>
                  <entry key="user">{4}</entry>
                  <entry key="passwd">{5}</entry>
                  <entry key="dbtype">postgis</entry>
                  <entry key="max connections">{6}</entry>
                  <entry key="Max connection idle time">{7}</entry>
                  <entry key="Evictor run periodicity">{8}</entry>
                  <entry key="validate connections">true</entry>
                  <entry key="Expose primary keys">false</entry>
                </connectionParameters>
              </dataStore>
              """.format(
            "foo",
            host,
            port,
            database,
            username,
            password,
            max_connections,
            max_connection_idle_time,
            evictor_run_periodicity,
        )

        expected_headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        rest_endpoint = "{endpoint}workspaces/{workspace}/datastores".format(
            endpoint=self.endpoint, workspace=self.workspace_name
        )

        self.assertRaises(
            requests.RequestException,
            self.engine.create_postgis_store,
            store_id,
            host,
            port,
            database,
            username,
            password,
            max_connections,
            max_connection_idle_time,
            evictor_run_periodicity,
        )
        mock_logger.error.assert_called()
        mock_post.assert_called_with(
            url=rest_endpoint, data=xml, headers=expected_headers, auth=self.auth
        )

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerCatalog.get_default_workspace"
    )
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    def test_create_layer_from_postgis_store(
        self, mock_store, mock_workspace, mock_post
    ):
        store_id = self.store_names[0]
        mock_store.return_value = {"success": True, "result": {"name": store_id}}
        mock_workspace.return_value = self.mock_workspaces[0]

        mock_post.return_value = MockResponse(201)

        table_name = "points"

        response = self.engine.create_layer_from_postgis_store(
            store_id=store_id, table=table_name, debug=False
        )

        expected_url = "{endpoint}workspaces/{w}/datastores/{s}/featuretypes".format(
            endpoint=self.endpoint, w=self.workspace_names[0], s=self.store_names[0]
        )
        expected_headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        # Validate response object
        self.assert_valid_response_object(response)

        self.assertTrue(response["success"])

        # Extract Result
        r = response["result"]

        self.assertIn("name", r)
        self.assertIn(self.store_names[0], r["name"])

        post_call_args = mock_post.call_args_list
        self.assertEqual(expected_url, post_call_args[0][1]["url"])
        self.assertEqual(expected_headers, post_call_args[0][1]["headers"])

        mock_store.assert_called_with(store_id=store_id, debug=False)

    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    def test_create_layer_from_postgis_store_fail_request(self, mock_store):
        mock_store.return_value = {"success": False, "error": ""}
        store_id = "{}:{}".format(self.workspace_names[0], self.store_names[0])

        table_name = "points"

        response = self.engine.create_layer_from_postgis_store(
            store_id=store_id, table=table_name, debug=False
        )

        # Validate response object
        self.assert_valid_response_object(response)

        self.assertFalse(response["success"])

        # Extract Result
        r = response["error"]

        self.assertIn("There is no store named", r)

        mock_store.assert_called_with(store_id, debug=False)

    @mock.patch("tethys_dataset_services.engines.geoserver_engine.requests.post")
    @mock.patch(
        "tethys_dataset_services.engines.geoserver_engine.GeoServerSpatialDatasetEngine.get_store"
    )
    def test_create_layer_from_postgis_store_not_201(self, mock_store, mock_post):
        mock_store.return_value = self.mock_stores[0]
        store_id = "{}:{}".format(self.workspace_names[0], self.store_names[0])

        mock_post.return_value = MockResponse(500)

        table_name = "points"

        response = self.engine.create_layer_from_postgis_store(
            store_id=store_id, table=table_name, debug=False
        )

        expected_url = "{endpoint}workspaces/{w}/datastores/{s}/featuretypes".format(
            endpoint=self.endpoint, w=self.workspace_names[0], s=self.store_names[0]
        )
        expected_headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        # Validate response object
        self.assert_valid_response_object(response)
        self.assertFalse(response["success"])

        post_call_args = mock_post.call_args_list
        self.assertEqual(expected_url, post_call_args[0][1]["url"])
        self.assertEqual(expected_headers, post_call_args[0][1]["headers"])

        mock_store.assert_called_with(store_id, debug=False)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGeoServerDatasetEngine("test_create_style"))
    runner = unittest.TextTestRunner()
    runner.run(suite)
