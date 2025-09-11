from builtins import *  # noqa: F403, F401
from jinja2 import Template
import logging
import os
import shutil
import tempfile
import pprint
import requests
from requests.auth import HTTPBasicAuth
from io import BytesIO
from urllib.parse import urlparse
from xml.etree import ElementTree
from zipfile import ZipFile, is_zipfile

import geoserver
from geoserver.catalog import Catalog as GeoServerCatalog
from geoserver.util import shapefile_and_friends

from ..utilities import ConvertDictToXml, ConvertXmlToDict
from ..base import SpatialDatasetEngine

log = logging.getLogger("tds.engines.geoserver")


class GeoServerSpatialDatasetEngine(SpatialDatasetEngine):
    """
    Definition for GeoServer Dataset Engine objects.
    """

    XML_PATH = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "resources",
        "geoserver",
        "xml_templates",
    )
    WARNING_STATUS_CODES = [403, 404]

    GWC_OP_SEED = "seed"
    GWC_OP_RESEED = "reseed"
    GWC_OP_TRUNCATE = "truncate"
    GWC_OP_MASS_TRUNCATE = "masstruncate"
    GWC_OPERATIONS = (GWC_OP_SEED, GWC_OP_RESEED, GWC_OP_TRUNCATE, GWC_OP_MASS_TRUNCATE)

    GWC_KILL_ALL = "all"
    GWC_KILL_RUNNING = "running"
    GWC_KILL_PENDING = "pending"
    GWC_KILL_OPERATIONS = (GWC_KILL_ALL, GWC_KILL_PENDING, GWC_KILL_RUNNING)

    GWC_STATUS_ABORTED = -1
    GWC_STATUS_PENDING = 0
    GWC_STATUS_RUNNING = 1
    GWC_STATUS_DONE = 2
    GWC_STATUS_MAP = {
        GWC_STATUS_ABORTED: "Aborted",
        GWC_STATUS_PENDING: "Pending",
        GWC_STATUS_RUNNING: "Running",
        GWC_STATUS_DONE: "Done",
    }

    # coverage types
    CT_AIG = "AIG"
    CT_ARC_GRID = "ArcGrid"
    CT_DTED = "DTED"
    CT_ECW = "ECW"
    CT_EHDR = "EHdr"
    CT_ENVIHDR = "ENVIHdr"
    CT_ERDASIMG = "ERDASImg"
    CT_GEOTIFF = "GeoTIFF"
    CT_GRASS_GRID = "GrassGrid"
    CT_GTOPO30 = "Gtopo30"
    CT_IMAGE_MOSAIC = "ImageMosaic"
    CT_IMAGE_PYRAMID = "ImagePyramid"
    CT_JP2MRSID = "JP2MrSID"
    CT_MRSID = "MrSID"
    CT_NETCDF = "NetCDF"
    CT_NITF = "NITF"
    CT_RPFTOC = "RPFTOC"
    CT_RST = "RST"
    CT_WORLD_IMAGE = "WorldImage"

    VALID_COVERAGE_TYPES = (
        CT_AIG,
        CT_ARC_GRID,
        CT_DTED,
        CT_ECW,
        CT_EHDR,
        CT_ENVIHDR,
        CT_ERDASIMG,
        CT_GEOTIFF,
        CT_GRASS_GRID,
        CT_GTOPO30,
        CT_IMAGE_MOSAIC,
        CT_IMAGE_PYRAMID,
        CT_JP2MRSID,
        CT_MRSID,
        CT_NETCDF,
        CT_NITF,
        CT_RPFTOC,
        CT_RST,
        CT_WORLD_IMAGE,
    )

    @property
    def type(self):
        """
        GeoServer Spatial Dataset Type
        """
        return "GEOSERVER"

    @property
    def gwc_endpoint(self):
        return self._gwc_endpoint

    @property
    def catalog(self):
        if not getattr(self, "_catalog", None):
            self._catalog = GeoServerCatalog(
                self.endpoint, username=self.username, password=self.password
            )
        return self._catalog

    def __init__(
        self,
        endpoint,
        apikey=None,
        username=None,
        password=None,
        public_endpoint=None,
        node_ports=None,
    ):
        """
        Default constructor for Dataset Engines.

        Args:
          api_endpoint (string): URL of the dataset service API endpoint (e.g.: www.host.com/api)
          apikey (string, optional): API key that will be used to authenticate with the dataset service.
          username (string, optional): Username that will be used to authenticate with the dataset service.
          password (string, optional): Password that will be used to authenticate with the dataset service.
          node_ports(list<int>, optional): A list of ports of each node in a clustered GeoServer deployment.
        """
        # Set custom property /geoserver/rest/ -> /geoserver/gwc/rest/
        if public_endpoint:
            self.public_endpoint = public_endpoint
        if "/" == endpoint[-1]:
            self._gwc_endpoint = endpoint.replace("rest", "gwc/rest")
        else:
            self._gwc_endpoint = endpoint.replace("rest", "gwc/rest/")

        self.node_ports = node_ports

        super(GeoServerSpatialDatasetEngine, self).__init__(
            endpoint=endpoint, apikey=apikey, username=username, password=password
        )

    def __del__(self):
        self.close()

    def _apply_changes_to_gs_object(self, attributes_dict, gs_object):
        # Make the changes
        for attribute, value in attributes_dict.items():
            if hasattr(gs_object, attribute):
                if attribute == "styles":
                    styles_objects = []

                    for style in attributes_dict["styles"]:
                        # Lookup by name and workspace
                        if ":" in style:
                            style_split = style.split(":")
                            styles_objects.append(
                                self.catalog.get_style(
                                    name=style_split[1], workspace=style_split[0]
                                )
                            )
                        # Lookup by name only
                        else:
                            styles_objects.append(self.catalog.get_style(name=style))

                    setattr(gs_object, "styles", styles_objects)

                elif attribute == "default_style":
                    style = attributes_dict["default_style"]

                    if ":" in style:
                        style_split = style.split(":")
                        style_object = self.catalog.get_style(
                            name=style_split[1], workspace=style_split[0]
                        )

                    # Lookup by name only
                    else:
                        style_object = self.catalog.get_style(name=style)

                    gs_object.default_style = style_object

                else:
                    setattr(gs_object, attribute, value)

        return gs_object

    def _assemble_url(self, *args):
        """
        Create a URL from all the args.
        """
        endpoint = self.endpoint

        # Eliminate trailing slash if necessary
        if endpoint[-1] == "/":
            endpoint = endpoint[:-1]

        pieces = list(args)
        pieces.insert(0, endpoint)
        return "/".join(pieces)

    def _get_non_rest_endpoint(self):
        """
        Get endpoint without the "rest".
        """
        endpoint = self.endpoint
        # Eliminate trailing slash if necessary
        if endpoint[-1] == "/":
            endpoint = endpoint[:-1]
        if endpoint[-5:] == "/rest":
            endpoint = endpoint[:-5]
        return endpoint

    def _get_wms_url(
        self,
        layer_id,
        style="",
        srs="EPSG:4326",
        bbox="-180,-90,180,90",
        version="1.1.0",
        width="512",
        height="512",
        output_format="image/png",
        tiled=False,
        transparent=True,
    ):
        """
        Assemble a WMS url.
        """
        endpoint = self._get_non_rest_endpoint()

        if tiled:
            tiled_option = "yes"
        else:
            tiled_option = "no"

        if transparent:
            transparent_option = "true"
        else:
            transparent_option = "false"

        wms_url = (
            "{0}/wms?service=WMS&version={1}&request=GetMap&"
            "layers={2}&styles={3}&"
            "transparent={10}&tiled={9}&"
            "srs={4}&bbox={5}&"
            "width={6}&height={7}&"
            "format={8}".format(
                endpoint,
                version,
                layer_id,
                style,
                srs,
                bbox,
                width,
                height,
                output_format,
                tiled_option,
                transparent_option,
            )
        )

        return wms_url

    def _get_wcs_url(
        self,
        resource_id,
        srs="EPSG:4326",
        bbox="-180,-90,180,90",
        output_format="png",
        namespace=None,
        width="512",
        height="512",
    ):
        """
        Assemble a WCS url.
        """
        endpoint = self._get_non_rest_endpoint()

        wcs_url = (
            "{0}/wcs?service=WCS&version=1.1.0&request=GetCoverage&"
            "identifier={1}&"
            "srs={2}&BoundingBox={3}&"
            "width={5}&height={6}&"
            "format={4}".format(
                endpoint, resource_id, srs, bbox, output_format, width, height
            )
        )

        if namespace and isinstance(namespace, str):
            wcs_url = "{0}&namespace={1}".format(wcs_url, namespace)

        return wcs_url

    def _get_wfs_url(self, resource_id, output_format="GML3"):
        """
        Assemble a WFS url.
        """
        endpoint = self._get_non_rest_endpoint()

        if output_format == "GML3":
            wfs_url = "{0}/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames={1}".format(
                endpoint, resource_id
            )
        elif output_format == "GML2":
            wfs_url = (
                "{0}/wfs?service=WFS&version=1.0.0&request=GetFeature&typeNames={1}&"
                "outputFormat=GML2".format(endpoint, resource_id)
            )
        else:
            wfs_url = (
                "{0}/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames={1}&"
                "outputFormat={2}".format(endpoint, resource_id, output_format)
            )

        return wfs_url

    def _get_node_endpoints(self, ports=None, public=True, gwc=False):
        node_endpoints = []
        if not gwc:
            endpoint = (
                self.public_endpoint
                if public and hasattr(self, "public_endpoint")
                else self.endpoint
            )
        else:
            endpoint = self.get_gwc_endpoint(public=public)

        endpoint = f"{endpoint}/" if not endpoint.endswith("/") else endpoint

        if ports is None:
            ports = self.node_ports
        log.debug(f"GeoServer Node Ports: {ports}")

        if ports is not None:
            gs_url = urlparse(endpoint)
            for port in ports:
                node_endpoints.append(
                    f"{gs_url.scheme}://{gs_url.hostname}:{port}{gs_url.path}"
                )
        else:
            node_endpoints.append(endpoint)
        return node_endpoints

    @staticmethod
    def _handle_debug(return_object, debug):
        """
        Handle debug
        """
        if debug:
            pprint.pprint(return_object)

    def _handle_delete(self, identifier, gs_object, purge, recurse, debug):
        """
        Handle delete calls
        """
        # Initialize response dictionary
        response_dict = {"success": False}
        if gs_object:
            try:
                # Execute
                self.catalog.delete(
                    config_object=gs_object, purge=purge, recurse=recurse
                )

                # Update response dictionary
                response_dict["success"] = True
                response_dict["result"] = None

            except geoserver.catalog.FailedRequestError as e:
                # Update response dictionary
                response_dict["success"] = False
                response_dict["error"] = str(e)

        else:
            # Update response dictionary
            response_dict["success"] = False
            response_dict["error"] = 'GeoServer object does not exist: "{0}".'.format(
                identifier
            )

        self._handle_debug(response_dict, debug)
        return response_dict

    def _handle_list(self, gs_objects, with_properties, debug):
        """
        Handle list calls
        """
        if not with_properties:
            names = []

            for gs_object in gs_objects:
                names.append(gs_object.name)

            # Assemble Response
            response_dict = {"success": True, "result": names}

            # Handle the debug and return
            self._handle_debug(response_dict, debug)
            return response_dict

        # Handle the debug and return
        gs_object_dicts = self._transcribe_geoserver_objects(gs_objects)

        # Assemble Response
        response_dict = {"success": True, "result": gs_object_dicts}

        self._handle_debug(response_dict, debug)
        return response_dict

    def _process_identifier(self, identifier):
        """
        Split identifier into name and workspace parts if applicable
        """
        # Assume no workspace and only name
        workspace = None
        name = identifier

        # Colon ':' is a delimiter between workspace and name i.e: workspace:name
        if ":" in identifier:
            workspace, name = identifier.split(":")

        return workspace, name

    def _transcribe_geoserver_objects(self, gs_object_list):
        """
        Convert a list of geoserver objects to a list of Python dictionaries.
        """
        gs_dict_list = []
        for gs_object in gs_object_list:
            gs_dict_list.append(self._transcribe_geoserver_object(gs_object))

        return gs_dict_list

    def _transcribe_geoserver_object(self, gs_object):
        """
        Convert geoserver objects to Python dictionaries.
        """
        # Constants
        NAMED_OBJECTS = ("store", "workspace")
        NAMED_OBJECTS_WITH_WORKSPACE = ("resource", "default_style")
        OMIT_ATTRIBUTES = (
            "writers",
            "attribution_object",
            "dirty",
            "dom",
            "save_method",
        )

        # Load into a dictionary
        object_dictionary = {}
        resource_object = None

        # Get the non-private attributes
        attributes = [
            a
            for a in dir(gs_object)
            if not a.startswith("__") and not a.startswith("_")
        ]

        for attribute in attributes:
            if not callable(getattr(gs_object, attribute)):
                # Handle special cases upfront
                if attribute in NAMED_OBJECTS:
                    sub_object = getattr(gs_object, attribute)
                    if not sub_object or isinstance(sub_object, str):
                        object_dictionary[attribute] = sub_object
                    else:
                        object_dictionary[attribute] = sub_object.name

                elif attribute in NAMED_OBJECTS_WITH_WORKSPACE:
                    # Append workspace if applicable
                    sub_object = getattr(gs_object, attribute)
                    # Stash resource for later use
                    if attribute == "resource":
                        resource_object = sub_object

                    if sub_object and not isinstance(sub_object, str):
                        if sub_object.workspace:
                            try:
                                object_dictionary[attribute] = "{0}:{1}".format(
                                    sub_object.workspace.name, sub_object.name
                                )
                            except AttributeError:
                                object_dictionary[attribute] = "{0}:{1}".format(
                                    sub_object.workspace, sub_object.name
                                )
                        else:
                            object_dictionary[attribute] = sub_object.name
                    elif isinstance(sub_object, str):
                        object_dictionary[attribute] = getattr(gs_object, attribute)

                elif attribute in OMIT_ATTRIBUTES:
                    # Omit these attributes
                    pass

                elif attribute == "catalog":
                    # Store URL in place of catalog
                    catalog_object = getattr(gs_object, "catalog")
                    object_dictionary[attribute] = catalog_object.service_url

                elif attribute == "styles":
                    styles = getattr(gs_object, attribute)
                    styles_names = []

                    for style in styles:
                        if style is not None:
                            if not isinstance(style, str):
                                if style.workspace:
                                    styles_names.append(
                                        "{0}:{1}".format(style.workspace, style.name)
                                    )
                                else:
                                    styles_names.append(style.name)
                            else:
                                styles_names = getattr(gs_object, attribute)

                    object_dictionary[attribute] = styles_names

                # Store attribute properties as is
                else:
                    object_dictionary[attribute] = getattr(gs_object, attribute)

        # Inject appropriate WFS and WMS URLs
        if "resource_type" in object_dictionary:
            # Feature Types Get WFS
            if object_dictionary["resource_type"] == "featureType":
                if object_dictionary["workspace"]:
                    resource_id = "{0}:{1}".format(
                        object_dictionary["workspace"], object_dictionary["name"]
                    )
                else:
                    resource_id = object_dictionary["name"]

                object_dictionary["wfs"] = {
                    "gml3": self._get_wfs_url(resource_id, "GML3"),
                    "gml2": self._get_wfs_url(resource_id, "GML2"),
                    "shapefile": self._get_wfs_url(resource_id, "shape-zip"),
                    "geojson": self._get_wfs_url(resource_id, "application/json"),
                    "geojsonp": self._get_wfs_url(resource_id, "text/javascript"),
                    "csv": self._get_wfs_url(resource_id, "csv"),
                }

            # Coverage Types Get WCS
            elif object_dictionary["resource_type"] == "coverage":
                workspace = None
                name = object_dictionary["name"]
                bbox = "-180,-90,180,90"
                srs = "EPSG:4326"
                width = "512"
                height = "512"

                if object_dictionary["workspace"]:
                    workspace = object_dictionary["workspace"]

                if resource_object and resource_object.native_bbox:
                    # Find the native bounding box
                    nbbox = resource_object.native_bbox
                    minx = nbbox[0]
                    maxx = nbbox[1]
                    miny = nbbox[2]
                    maxy = nbbox[3]
                    srs = resource_object.projection
                    bbox = "{0},{1},{2},{3}".format(minx, miny, maxx, maxy)

                    # Resize the width to be proportionate to the image aspect ratio
                    aspect_ratio = (float(maxx) - float(minx)) / (
                        float(maxy) - float(miny)
                    )
                    width = str(int(aspect_ratio * float(height)))

                object_dictionary["wcs"] = {
                    "png": self._get_wcs_url(
                        name,
                        output_format="png",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "gif": self._get_wcs_url(
                        name,
                        output_format="gif",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "jpeg": self._get_wcs_url(
                        name,
                        output_format="jpeg",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "tiff": self._get_wcs_url(
                        name,
                        output_format="tif",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "bmp": self._get_wcs_url(
                        name,
                        output_format="bmp",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "geotiff": self._get_wcs_url(
                        name,
                        output_format="geotiff",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "gtopo30": self._get_wcs_url(
                        name,
                        output_format="gtopo30",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "arcgrid": self._get_wcs_url(
                        name,
                        output_format="ArcGrid",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                    "arcgrid_gz": self._get_wcs_url(
                        name,
                        output_format="ArcGrid-GZIP",
                        namespace=workspace,
                        srs=srs,
                        bbox=bbox,
                    ),
                }

            elif object_dictionary["resource_type"] == "layer":
                # Defaults
                bbox = "-180,-90,180,90"
                srs = "EPSG:4326"
                width = "512"
                height = "512"
                style = ""

                # Layer and style
                layer = object_dictionary["name"]
                if "default_style" in object_dictionary:
                    style = object_dictionary["default_style"]

                # Try to extract the bounding box from the resource which was saved earlier
                if resource_object and resource_object.native_bbox:
                    # Find the native bounding box
                    nbbox = resource_object.native_bbox
                    minx = nbbox[0]
                    maxx = nbbox[1]
                    miny = nbbox[2]
                    maxy = nbbox[3]
                    srs = resource_object.projection
                    bbox = "{0},{1},{2},{3}".format(minx, miny, maxx, maxy)

                    # Resize the width to be proportionate to the image aspect ratio
                    aspect_ratio = (float(maxx) - float(minx)) / (
                        float(maxy) - float(miny)
                    )
                    width = str(int(aspect_ratio * float(height)))

                object_dictionary["wms"] = {
                    "png": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/png",
                    ),
                    "png8": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/png8",
                    ),
                    "jpeg": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/jpeg",
                    ),
                    "gif": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/gif",
                    ),
                    "tiff": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/tiff",
                    ),
                    "tiff8": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/tiff8",
                    ),
                    "geotiff": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/geotiff",
                    ),
                    "geotiff8": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/geotiff8",
                    ),
                    "svg": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/svg",
                    ),
                    "pdf": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="application/pdf",
                    ),
                    "georss": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="rss",
                    ),
                    "kml": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="kml",
                    ),
                    "kmz": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="kmz",
                    ),
                    "openlayers": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="application/openlayers",
                    ),
                }

            elif object_dictionary["resource_type"] == "layerGroup":
                # Defaults
                bbox = "-180,-90,180,90"
                srs = "EPSG:4326"
                width = "512"
                height = "512"
                style = ""

                # Layer and style
                layer = object_dictionary["name"]
                if "default_style" in object_dictionary:
                    style = object_dictionary["default_style"]

                # Try to extract the bounding box from the resource which was saved earlier
                if "bounds" in object_dictionary and object_dictionary["bounds"]:
                    # Find the native bounding box
                    nbbox = object_dictionary["bounds"]
                    minx = nbbox[0]
                    maxx = nbbox[1]
                    miny = nbbox[2]
                    maxy = nbbox[3]
                    srs = nbbox[4]
                    bbox = "{0},{1},{2},{3}".format(minx, miny, maxx, maxy)

                    # Resize the width to be proportionate to the image aspect ratio
                    aspect_ratio = (float(maxx) - float(minx)) / (
                        float(maxy) - float(miny)
                    )
                    width = str(int(aspect_ratio * float(height)))

                object_dictionary["wms"] = {
                    "png": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/png",
                    ),
                    "png8": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/png8",
                    ),
                    "jpeg": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/jpeg",
                    ),
                    "gif": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/gif",
                    ),
                    "tiff": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/tiff",
                    ),
                    "tiff8": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/tiff8",
                    ),
                    "geptiff": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/geotiff",
                    ),
                    "geotiff8": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/geotiff8",
                    ),
                    "svg": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="image/svg",
                    ),
                    "pdf": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="application/pdf",
                    ),
                    "georss": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="rss",
                    ),
                    "kml": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="kml",
                    ),
                    "kmz": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="kmz",
                    ),
                    "openlayers": self._get_wms_url(
                        layer,
                        style,
                        bbox=bbox,
                        srs=srs,
                        width=width,
                        height=height,
                        output_format="application/openlayers",
                    ),
                }

        return object_dictionary

    def get_gwc_endpoint(self, public=True):
        """
        Returns the GeoServer endpoint for GWC services (with trailing slash).

        Args:
            public (bool): return with the public endpoint if True.
        """
        if public and hasattr(self, "public_endpoint"):
            gs_endpoint = self.public_endpoint.replace("rest", "gwc/rest")
        else:
            gs_endpoint = self._gwc_endpoint

        # Add trailing slash for consistency.
        if not gs_endpoint.endswith("/"):
            gs_endpoint += "/"

        return gs_endpoint

    def get_ows_endpoint(self, workspace, public=True):
        """
        Returns the GeoServer endpoint for OWS services (with trailing slash).

        Args:
            workspace (str): the name of the workspace
            public (bool): return with the public endpoint if True.
        """
        gs_endpoint = (
            self.public_endpoint
            if public and hasattr(self, "public_endpoint")
            else self.endpoint
        )
        gs_endpoint = gs_endpoint.replace("rest", "{0}/ows".format(workspace))

        # Add trailing slash for consistency.
        if not gs_endpoint.endswith("/"):
            gs_endpoint += "/"
        return gs_endpoint

    def get_wms_endpoint(self, public=True):
        """
        Returns the GeoServer endpoint for WMS services (with trailing slash).

        Args:
            public (bool): return with the public endpoint if True.
        """
        gs_endpoint = (
            self.public_endpoint
            if public and hasattr(self, "public_endpoint")
            else self.endpoint
        )
        gs_endpoint = gs_endpoint.replace("rest", "wms")

        # Add trailing slash for consistency.
        if not gs_endpoint.endswith("/"):
            gs_endpoint += "/"
        return gs_endpoint

    def close(self):
        self.catalog.client.close()

    def reload(self, ports=None, public=True):
        """
        Reload the configuration from disk.

        Args:
            ports (iterable): A tuple or list of integers representing the ports on which different instances of
                              GeoServer are running in a clustered GeoServer configuration.
            public (bool): Use the public geoserver endpoint if True, otherwise use the internal endpoint.
        """

        node_endpoints = self._get_node_endpoints(ports=ports, public=public)
        log.debug("Catalog Reload URLS: {0}".format(node_endpoints))

        response_dict = {"success": True, "result": None, "error": []}
        for endpoint in node_endpoints:
            try:
                response = requests.post(
                    f"{endpoint}reload", auth=(self.username, self.password)
                )

                if response.status_code != 200:
                    msg = "Catalog Reload Status Code {0}: {1}".format(
                        response.status_code, response.text
                    )
                    exception = requests.RequestException(msg, response=response)
                    log.error(exception)
                    response_dict["success"] = False
                    response_dict["error"].append(msg)
            except requests.ConnectionError:
                log.warning("Catalog could not be reloaded on a GeoServer node.")

        (
            response_dict.pop("error", None)
            if not response_dict["error"]
            else response_dict.pop("result", None)
        )
        return response_dict

    def gwc_reload(self, ports=None, public=True):
        """
        Reload the GeoWebCache configuration from disk.

        Args:
            ports (iterable): A tuple or list of integers representing the ports on which different instances of
                                GeoServer are running in a clustered GeoServer configuration.
            public (bool): Use the public geoserver endpoint if True, otherwise use the internal
                                    endpoint.
        """
        node_endpoints = self._get_node_endpoints(ports=ports, public=public, gwc=True)
        log.debug("GeoWebCache Reload URLS: {0}".format(node_endpoints))

        response_dict = {"success": True, "result": None, "error": []}
        for endpoint in node_endpoints:
            retries_remaining = 3
            while retries_remaining > 0:
                try:
                    response = requests.post(
                        f"{endpoint}reload", auth=(self.username, self.password)
                    )

                    if response.status_code != 200:
                        msg = "GeoWebCache Reload Status Code {0}: {1}".format(
                            response.status_code, response.text
                        )
                        exception = requests.RequestException(msg, response=response)
                        log.error(exception)
                        retries_remaining -= 1
                        if retries_remaining == 0:
                            response_dict["success"] = False
                            response_dict["error"].append(msg)
                        continue

                except requests.ConnectionError:
                    log.warning(
                        "GeoWebCache could not be reloaded on a GeoServer node."
                    )
                    retries_remaining -= 1

                break

        (
            response_dict.pop("error", None)
            if not response_dict["error"]
            else response_dict.pop("result", None)
        )
        return response_dict

    def list_resources(
        self, with_properties=False, store=None, workspace=None, debug=False
    ):
        """
        List the names of all resources available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of resource dictionaries instead of a list of resource names.
          store (string, optional): Return only resources belonging to a certain store.
          workspace (string, optional): Return only resources belonging to a certain workspace.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_resource()

          response = engine.list_resource(store="example_store")

          response = engine.list_resource(with_properties=True, workspace="example_workspace")

        """
        try:
            resource_objects = self.catalog.get_resources(
                stores=store, workspaces=workspace
            )
            return self._handle_list(resource_objects, with_properties, debug)
        except geoserver.catalog.AmbiguousRequestError as e:
            response_object = {"success": False, "error": str(e)}
        except TypeError:
            response_object = {
                "success": False,
                "error": 'Multiple stores found named "{0}".'.format(store),
            }
        self._handle_debug(response_object, debug)
        return response_object

    def list_layers(self, with_properties=False, debug=False):
        """
        List names of all layers available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of layer dictionaries instead of a list of layer names.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_layers()

          response = engine.list_layers(with_properties=True)
        """
        layer_objects = self.catalog.get_layers()
        return self._handle_list(layer_objects, with_properties, debug)

    def list_layer_groups(self, with_properties=False, debug=False):
        """
        List the names of all layer groups available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of layer group dictionaries instead of a list of layer group names.  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_layer_groups()

          response = engine.list_layer_groups(with_properties=True)
        """
        layer_group_objects = self.catalog.get_layergroups()
        return self._handle_list(layer_group_objects, with_properties, debug)

    def list_workspaces(self, with_properties=False, debug=False):
        """
        List the names of all workspaces available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of workspace dictionaries instead of a list of workspace names.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_workspaces()

          response = engine.list_workspaces(with_properties=True)
        """
        workspaces = self.catalog.get_workspaces()
        return self._handle_list(workspaces, with_properties, debug)

    def list_stores(self, workspace=None, with_properties=False, debug=False):
        """
        List the names of all stores available from the spatial dataset service.

        Args:
          workspace (string, optional): List long stores belonging to this workspace.
          with_properties (bool, optional): Return list of store dictionaries instead of a list of store names.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_stores()

          response = engine.list_stores(workspace='example_workspace", with_properties=True)
        """
        if workspace is None:
            workspace = []
        elif isinstance(workspace, str):
            workspace = [workspace]

        try:
            stores = self.catalog.get_stores(workspaces=workspace)
            return self._handle_list(stores, with_properties, debug)

        except AttributeError:
            response_dict = {
                "success": False,
                "error": 'Invalid workspace "{0}".'.format(workspace),
            }
        self._handle_debug(response_dict, debug)
        return response_dict

    def list_styles(self, workspace=None, with_properties=False, debug=False):
        """
        List the names of all styles available from the spatial dataset service.

        Args:
          workspace (string): Return only resources belonging to a certain workspace.
          with_properties (bool, optional): Return list of style dictionaries instead of a list of style names.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_styles()

          response = engine.list_styles(with_properties=True)
        """
        if workspace is None:
            workspace = []
        elif isinstance(workspace, str):
            workspace = [workspace]

        styles = self.catalog.get_styles(workspaces=workspace)
        return self._handle_list(styles, with_properties, debug)

    def get_resource(self, resource_id, store_id=None, debug=False):
        """
        Retrieve a resource object.

        Args:
          resource_id (string): Identifier of the resource to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          store (string, optional): Get resource from this store.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_resource('example_workspace:resource_name')

          response = engine.get_resource('resource_name', store='example_store')

        """
        # Process identifier
        workspace, name = self._process_identifier(resource_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Get resource
        try:
            resource = self.catalog.get_resource(
                name=name, store=store_id, workspace=workspace
            )
            if not resource:
                response_dict = {
                    "success": False,
                    "error": 'Resource "{0}" not found.'.format(resource_id),
                }
            else:
                resource_dict = self._transcribe_geoserver_object(resource)

                # Assemble Response
                response_dict = {"success": True, "result": resource_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_layer(self, layer_id, store_id=None, debug=False):
        """
        Retrieve a layer object.

        Args:
          layer_id (string): Identifier of the layer to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          store_id (string, optional): Return only resources belonging to a certain store.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_layer('layer_name')

          response = engine.get_layer('workspace_name:layer_name')
        """
        try:
            # Get layer
            layer = self.catalog.get_layer(name=layer_id)
            if layer and store_id:
                layer.store = store_id
            if not layer:
                response_dict = {
                    "success": False,
                    "error": 'Layer "{0}" not found.'.format(layer_id),
                }
            else:
                layer_dict = self._transcribe_geoserver_object(layer)

                # Get layer caching properties (gsconfig doesn't support this)
                gwc_url = "{0}layers/{1}.xml".format(self.gwc_endpoint, layer_id)
                auth = (self.username, self.password)
                r = requests.get(gwc_url, auth=auth)

                if r.status_code == 200:
                    root = ElementTree.XML(r.text)
                    tile_caching_dict = ConvertXmlToDict(root)
                    layer_dict["tile_caching"] = tile_caching_dict["GeoServerLayer"]

                # Assemble Response
                response_dict = {"success": True, "result": layer_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_layer_group(self, layer_group_id, debug=False):
        """
        Retrieve a layer group object.

        Args:
          layer_group_id (string): Identifier of the layer group to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_layer_group('layer_group_name')

          response = engine.get_layer_group('workspace_name:layer_group_name')
        """
        workspace, name = self._process_identifier(layer_group_id)

        # If workspaces is passed as None, get_layergroups will return as None
        if workspace is None:
            workspaces = []
        else:
            workspaces = [workspace]

        try:
            # Get layer group
            # Using get_layergroups instead of get_layergroup b/c get_layergroup
            # cannot handle the case where workspaces is None (always returns None)
            layer_groups = self.catalog.get_layergroups(
                names=name, workspaces=workspaces
            )
            layer_group = self.catalog._return_first_item(layer_groups)

            if not layer_group:
                response_dict = {
                    "success": False,
                    "error": 'Layer Group "{0}" not found.'.format(layer_group_id),
                }
            else:
                layer_group_dict = self._transcribe_geoserver_object(layer_group)

                # Assemble Response
                response_dict = {"success": True, "result": layer_group_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_store(self, store_id, debug=False):
        """
        Retrieve a store object.

        Args:
          store_id (string): Identifier of the store to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_store('store_name')

          response = engine.get_store('workspace_name:store_name')
        """
        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        try:
            # Get resource
            store = self.catalog.get_store(name=name, workspace=workspace)

            if not store:
                response_dict = {
                    "success": False,
                    "error": 'Store "{0}" not found.'.format(store_id),
                }
            else:
                store_dict = self._transcribe_geoserver_object(store)

                # Assemble Response
                response_dict = {"success": True, "result": store_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_workspace(self, workspace_id, debug=False):
        """
        Retrieve a workspace object.

        Args:
          workspace_id (string): Identifier of the workspace to retrieve.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_workspace('workspace_name')
        """
        try:
            # Get resource
            workspace = self.catalog.get_workspace(name=workspace_id)

            if not workspace:
                response_dict = {
                    "success": False,
                    "error": 'Workspace "{0}" not found.'.format(workspace_id),
                }
            else:
                workspace_dict = self._transcribe_geoserver_object(workspace)

                # Assemble Response
                response_dict = {"success": True, "result": workspace_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_style(self, style_id, debug=False):
        """
        Retrieve a style object.

        Args:
          style_id (string): Identifier of the style to retrieve.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_style('style_name')

        """
        # Process identifier
        workspace, name = self._process_identifier(style_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        try:
            # Get style
            style = self.catalog.get_style(name=name, workspace=workspace)

            if not style:
                response_dict = {
                    "success": False,
                    "error": 'Workspace "{0}" not found.'.format(style_id),
                }
            else:
                style_dict = self._transcribe_geoserver_object(style)

                # Assemble Response
                response_dict = {"success": True, "result": style_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_layer_extent(
        self, store_id, feature_name, native=False, buffer_factor=1.000001
    ):
        """
        Get the legend extent for the given layer.

        Args:
            datastore_name: Name of a GeoServer data store (assumption: the datastore belongs to the workspace).
            feature_name: Name of the feature type. Will also be used to name the layer.
            native (bool): True if the native projection extent should be used. Defaults to False.
            buffer_factor(float): Apply a buffer around the bounding box.
        """  # noqa: E501
        # Process identifier
        workspace, datastore_name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        url = (
            self.endpoint
            + "workspaces/"
            + workspace
            + "/datastores/"
            + datastore_name
            + "/featuretypes/"
            + feature_name
            + ".json"
        )

        response = requests.get(url, auth=(self.username, self.password))

        if response.status_code != 200:
            msg = "Get Layer Extent Status Code {0}: {1}".format(
                response.status_code, response.text
            )
            exception = requests.RequestException(msg, response=response)
            log.error(exception)
            raise exception

        # Get the JSON
        json = response.json()

        # Default bounding box
        bbox = None
        extent = [-128.583984375, 22.1874049914, -64.423828125, 52.1065051908]

        # Extract bounding box
        if "featureType" in json:
            if native:
                if "nativeBoundingBox" in json["featureType"]:
                    bbox = json["featureType"]["nativeBoundingBox"]
            else:
                if "latLonBoundingBox" in json["featureType"]:
                    bbox = json["featureType"]["latLonBoundingBox"]

        if bbox is not None:
            # minx, miny, maxx, maxy
            extent = [
                bbox["minx"] / buffer_factor,
                bbox["miny"] / buffer_factor,
                bbox["maxx"] * buffer_factor,
                bbox["maxy"] * buffer_factor,
            ]

        return extent

    def link_sqlalchemy_db_to_geoserver(
        self,
        store_id,
        sqlalchemy_engine,
        max_connections=5,
        max_connection_idle_time=30,
        evictor_run_periodicity=30,
        validate_connections=True,
        docker=False,
        debug=False,
        docker_ip_address="172.17.0.1",
    ):
        """
        Helper function to simplify linking postgis databases to geoservers using the sqlalchemy engine object.

        Args:
          store_id (string): Identifier for the store to add the resource to. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.  # noqa: E501
          sqlalchemy_engine (sqlalchemy_engine): An SQLAlchemy engine object.
          docker (bool, optional): Set to True if the database and geoserver are running in a Docker container. Defaults to False.  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
          docker_ip_address (str, optional): Override the docker network ip address. Defaults to '172.17.0.1'.

        Returns:
          (dict): Response dictionary
        """
        params = dict(
            store_id=store_id,
            **sqlalchemy_engine.url.translate_connect_args(),
            max_connections=max_connections,
            max_connection_idle_time=max_connection_idle_time,
            evictor_run_periodicity=evictor_run_periodicity,
            validate_connections=validate_connections,
            debug=debug,
        )

        if docker:
            params["host"] = docker_ip_address

        response = self.create_postgis_store(**params)
        return response

    def create_postgis_store(
        self,
        store_id,
        host,
        port,
        database,
        username,
        password,
        max_connections=5,
        max_connection_idle_time=30,
        evictor_run_periodicity=30,
        validate_connections=True,
        expose_primary_keys=False,
        debug=False,
    ):
        """
        Use this method to link an existing PostGIS database to GeoServer as a feature store. Note that this method only works for data in vector formats.

        Args:
          store_id (string): Identifier for the store to add the resource to. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
          host (string): Host of the PostGIS database (e.g.: 'www.example.com').
          port (string): Port of the PostGIS database (e.g.: '5432')
          database (string): Name of the database.
          username (string): Database user that has access to the database.
          password (string): Password of database user.
          max_connections (int, optional): Maximum number of connections allowed in connection pool. Defaults to 5.
          max_connection_idle_time (int, optional): Number of seconds a connections can stay idle before the evictor considers closing it. Defaults to 30 seconds.
          evictor_run_periodicity (int, optional): Number of seconds between idle connection evictor runs. Defaults to 30 seconds.
          validate_connections (bool, optional): Test connections before using. Defaults to True.
          expose_primary_keys (bool, optional):
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          engine.create_postgis_store(store_id='workspace:name', host='localhost', port='5432', database='database_name', username='user', password='pass')

        """  # noqa: E501
        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Create the store
        xml = f"""
              <dataStore>
                <name>{name}</name>
                <connectionParameters>
                  <entry key="host">{host}</entry>
                  <entry key="port">{port}</entry>
                  <entry key="database">{database}</entry>
                  <entry key="user">{username}</entry>
                  <entry key="passwd">{password}</entry>
                  <entry key="dbtype">postgis</entry>
                  <entry key="max connections">{max_connections}</entry>
                  <entry key="Max connection idle time">{max_connection_idle_time}</entry>
                  <entry key="Evictor run periodicity">{evictor_run_periodicity}</entry>
                  <entry key="validate connections">{str(validate_connections).lower()}</entry>
                  <entry key="Expose primary keys">{str(expose_primary_keys).lower()}</entry>
                </connectionParameters>
              </dataStore>
              """

        # Prepare headers
        headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        # Prepare URL to create store
        url = self._assemble_url("workspaces", workspace, "datastores")

        # Execute: POST /workspaces/<ws>/datastores
        response = requests.post(
            url=url, data=xml, headers=headers, auth=(self.username, self.password)
        )

        # Return with error if this doesn't work
        if response.status_code != 201:
            msg = "Create Postgis Store Status Code {0}: {1}".format(
                response.status_code, response.text
            )
            exception = requests.RequestException(msg, response=response)
            log.error(exception)
            raise exception

        # Wrap up successfully with new store created
        response_dict = self.get_store(store_id, debug)

        return response_dict

    def create_layer_from_postgis_store(self, store_id, table, layer_name=None, debug=False):
        """
        Add an existing PostGIS table as a feature resource to a PostGIS store that already exists.

        Args:
            store_id (str): Identifier for the store to add the resource to. This can be a store name,
                or "workspace:store_name" combo (e.g.: "name" or "workspace:name"). If the workspace
                is not specified, the catalog's default workspace is used.
            table (str): The underlying table name in the PostGIS database. A layer (feature resource)
                will be created referencing this table.
            layer_name (str, optional): If provided, this name will be used for the newly created layer
                in GeoServer. If not provided, defaults to the same as 'table'.
            debug (bool, optional): Pretty print the response dictionary to the console for debugging.
                Defaults to False.

        Returns:
            dict: A response dictionary with 'success', 'result', and/or 'error' keys.

        Examples:
            # Use the table name for layer:
            engine.create_layer_from_postgis_store(
                store_id='workspace:store_name',
                table='table_name'
            )

            # Provide a custom layer name:
            engine.create_layer_from_postgis_store(
                store_id='workspace:store_name',
                table='table_name',
                layer_name='my_custom_layer'
            )
        """
        # Extract (workspace, store_name) from the store_id
        workspace, store_name = self._process_identifier(store_id)
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Verify the store exists
        store_info = self.get_store(store_id, debug=debug)
        if not store_info["success"]:
            message = f"There is no store named '{store_name}'"
            if workspace:
                message += f" in {workspace}"
            return {"success": False, "error": message}

        # If no layer_name was provided, default to the PostGIS table name
        if not layer_name:
            layer_name = table

        # Create an XML body for the new feature type in GeoServer
        # The <name> field sets the GeoServer layer (and resource) name.
        xml_body = f"""
            <featureType>
                <name>{layer_name}</name>
                <nativeName>{table}</nativeName>
            </featureType>
        """

        headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        # POST /workspaces/<workspace>/datastores/<store_name>/featuretypes
        url = self._assemble_url(
            "workspaces", workspace, "datastores", store_name, "featuretypes"
        )
        response = requests.post(
            url=url,
            data=xml_body,
            headers=headers,
            auth=HTTPBasicAuth(username=self.username, password=self.password),
        )

        if response.status_code != 201:
            response_dict = {
                "success": False,
                "error": f"{response.reason}({response.status_code}): {response.text}",
            }
            self._handle_debug(response_dict, debug)
            return response_dict

        # Optionally return the store info, or you could directly query the new layer if desired
        response_dict = self.get_store(store_id=store_id, debug=debug)
        self._handle_debug(response_dict, debug)
        return response_dict

    def create_sql_view_layer(
        self,
        store_id,
        layer_name,
        geometry_type,
        srid,
        sql,
        default_style,
        geometry_name="geometry",
        other_styles=None,
        parameters=None,
        reload_public=False,
        debug=False,
        *,
        enable_gwc=True,
        gwc_method="AUTO"
    ):
        """
        Direct call to GeoServer REST API to create SQL View feature types and layers.

        Args:
            store_id (string): Identifier of existing postgis store with tables that will be queried by the sql view. (e.g.: "store_name" or "workspace:store_name").
            layer_name (string): Identifier of the sql view layer to create. (The layer will be created on the workspace of the existing store).
            geometry_name: Name of the PostGIS column/field of type geom.
            geometry_type: Type of geometry in geometry field (e.g.: Point, LineString)
            srid (int): EPSG spatial reference id. EPSG spatial reference ID.
            sql: The SQL query that defines the feature type.
            default_style: The name of the default style. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
            other_styles: A list of other default style names. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
            parameters: A list of parameter dictionaries { name, default_value, regex_validator }.
            reload_public: (bool, optional): Reload the catalog using the public endpoint. Defaults to False.
            debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
            enable_gwc (bool, keyword-only): If True, create/modify the GWC layer after the feature type is created. Default: True (backward compatible).
            gwc_method (str, keyword-only):
                One of {"AUTO", "POST", "PUT"}:
                    - "AUTO": probe and pick the correct method (POST=modify, PUT=create).
                    - "POST": force modify.
                    - "PUT":  force create.
                Default: "AUTO".
        """  # noqa: E501
        # Process identifier
        workspace, store_name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # use store's workspace as default for layer
        layer_id = f"{workspace}:{layer_name}" if ":" not in layer_name else layer_name

        # Template context
        context = {
            "workspace": workspace,
            "feature_name": layer_name,
            "datastore_name": store_name,
            "geoserver_rest_endpoint": self.endpoint,
            "sql": sql,
            "geometry_name": geometry_name,
            "geometry_type": geometry_type,
            "srid": srid,
            "parameters": parameters or [],
            "default_style": default_style,
            "other_styles": other_styles or [],
        }

        # Open sql view template
        sql_view_path = os.path.join(self.XML_PATH, "sql_view_template.xml")
        url = self._assemble_url(
            "workspaces", workspace, "datastores", store_name, "featuretypes"
        )
        headers = {"Content-type": "text/xml"}
        with open(sql_view_path, "r") as sql_view_file:
            text = sql_view_file.read()
            template = Template(text)
            xml = template.render(context)

        retries_remaining = 3
        while retries_remaining > 0:
            response = requests.post(
                url,
                headers=headers,
                auth=(self.username, self.password),
                data=xml,
            )

            # Raise an exception if status code is not what we expect
            if response.status_code == 201:
                log.info("Successfully created featuretype {}".format(layer_name))
                break
            if response.status_code == 500 and "already exists" in response.text:
                break
            else:
                retries_remaining -= 1
                if retries_remaining == 0:
                    msg = "Create Feature Type Status Code {0}: {1}".format(
                        response.status_code, response.text
                    )
                    exception = requests.RequestException(msg, response=response)
                    log.error(exception)
                    raise exception

        # Reload before attempting to update styles to avoid issues
        self.reload(public=reload_public)

        # Add styles to new layer
        self.update_layer_styles(
            layer_id=layer_id, default_style=default_style, other_styles=other_styles
        )

        # GeoWebCache Settings
        if enable_gwc:
            gwc_layer_path = os.path.join(self.XML_PATH, "gwc_layer_template.xml")
            gwc_url = (
                self.get_gwc_endpoint(public=False)
                + "layers/"
                + workspace
                + ":"
                + layer_name
                + ".xml"
            )
            gwc_headers = {"Content-type": "text/xml"}
            with open(gwc_layer_path, "r") as gwc_layer_file:
                text = gwc_layer_file.read()
                template = Template(text)
                xml = template.render(context)

            # Decide method, aligned with current GWC REST:
            # PUT => add new layer, POST => modify existing layer
            method = (gwc_method or "AUTO").upper()
            if method not in {"AUTO", "POST", "PUT"}:
                raise ValueError("gwc_method must be one of 'AUTO', 'POST', or 'PUT'")

            if method == "AUTO":
                try:
                    probe = requests.get(
                        gwc_url,
                        auth=(self.username, self.password),
                        headers={"Accept": "application/xml"},
                    )
                    # Exists? -> POST (modify). Missing? -> PUT (create).
                    if probe.status_code == 200:
                        method_to_use = "POST"
                    elif probe.status_code == 404:
                        method_to_use = "PUT"
                    else:
                        method_to_use = "POST"  # safe default
                except Exception:
                    method_to_use = "POST"  # safe default if probe fails
            else:
                method_to_use = method

            retries_remaining = 3
            put_fallback_done = False

            while retries_remaining > 0:
                if method_to_use == "PUT":
                    resp = requests.put(
                        gwc_url,
                        headers=gwc_headers,
                        auth=(self.username, self.password),
                        data=xml,
                    )
                    ok = resp.status_code == 200  # docs show 200 on success
                else:  # POST (modify)
                    resp = requests.post(
                        gwc_url,
                        headers=gwc_headers,
                        auth=(self.username, self.password),
                        data=xml,
                    )
                    ok = resp.status_code == 200

                if ok:
                    log.info("Successfully applied GeoWebCache layer settings for %s", layer_name)
                    break

                # If trying PUT but the layer already exists, switch once to POST
                if (
                    method_to_use == "PUT"
                    and not put_fallback_done
                    and (
                        resp.status_code in (405, 409)
                        or "already exists" in (resp.text or "").lower()
                    )
                ):
                    log.info("GWC layer %s already exists; switching to POST.", layer_name)
                    method_to_use = "POST"
                    put_fallback_done = True
                    continue

                log.warning("GWC returned %s. %s\n", resp.status_code, resp.text)
                retries_remaining -= 1
                if retries_remaining == 0:
                    msg = "Create/Update GWC Layer Status Code {0}: {1}".format(
                        resp.status_code, resp.text
                    )
                    exception = requests.RequestException(msg, response=resp)
                    log.error(exception)
                    raise exception

        response_dict = self.get_layer(layer_id, store_name, debug=debug)
        return response_dict

    def create_shapefile_resource(self, store_id, shapefile_base=None, shapefile_zip=None, shapefile_upload=None,
                                  overwrite=False, charset=None, default_style=None, debug=False):
        """
        Use this method to add shapefile resources to GeoServer.

        This method will result in the creation of three items: a feature type store, a feature type resource, and a layer. If store_id references a store that does not exist, it will be created. The feature type resource and the subsequent layer will be created with the same name as the feature type store. Provide shapefile with either shapefile_base, shapefile_zip, or shapefile_upload arguments.  # noqa: E501

        Args:
          store_id (string): Identifier for the store to add the resource to. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.  # noqa: E501
          shapefile_base (string, optional): Path to shapefile base name (e.g.: "/path/base" for shapefile at "/path/base.shp")
          shapefile_zip (string, optional): Path to a zip file containing the shapefile and side cars.
          shapefile_upload (FileUpload list, optional): A list of Django FileUpload objects containing a shapefile and side cars that have been uploaded via multipart/form-data form.  # noqa: E501
          overwrite (bool, optional): Overwrite the file if it already exists.
          charset (string, optional): Specify the character encoding of the file being uploaded (e.g.: ISO-8559-1).
          default_style (string, optional): The name of the default style to apply to the layer. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          # For example.shp (path to file but omit the .shp extension)

          shapefile_base = "/path/to/shapefile/example"

          response = engine.create_shapefile_resource(store_id='workspace:store_name', shapefile_base=shapefile_base)

          # Using zip

          shapefile_zip = "/path/to/shapefile/example.zip"

          response = engine.create_shapefile_resource(store_id='workspace:store_name', shapefile_zip=shapefile_zip)

          # Using upload

          file_list = request.FILES.getlist('files')

          response = engine.create_shapefile_resource(store_id='workspace:store_name', shapefile_upload=file_list)

        """
        # Validate shapefile arguments
        arg_value_error_msg = (
            'Exactly one of the "shapefile_base", "shapefile_zip", '
            'or "shapefile_upload" arguments must be specified. '
        )

        if not shapefile_base and not shapefile_zip and not shapefile_upload:
            raise ValueError(arg_value_error_msg + "None given.")

        elif shapefile_zip and shapefile_upload and shapefile_base:
            raise ValueError(
                arg_value_error_msg + '"shapefile_base", "shapefile_zip", and '
                '"shapefile_upload" given.'
            )

        elif shapefile_base and shapefile_zip:
            raise ValueError(
                arg_value_error_msg + '"shapefile_base" and "shapefile_zip" given.'
            )

        elif shapefile_base and shapefile_upload:
            raise ValueError(
                arg_value_error_msg + '"shapefile_base" and "shapefile_upload" given.'
            )

        elif shapefile_zip and shapefile_upload:
            raise ValueError(
                arg_value_error_msg + '"shapefile_zip" and "shapefile_upload" given.'
            )

        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Throw error if overwrite is not true and store already exists
        if not overwrite:
            try:
                self.catalog.get_store(name=name, workspace=workspace)
                message = "There is already a store named " + name
                if workspace:
                    message += " in " + workspace

                response_dict = {"success": False, "error": message}

                self._handle_debug(response_dict, debug)
                return response_dict

            except geoserver.catalog.FailedRequestError:
                pass

        # Prepare files
        temp_archive = None
        zip_file_in_memory = None

        # Shapefile Base Case
        if shapefile_base:
            shapefile_plus_sidecars = shapefile_and_friends(shapefile_base)
            temp_archive = "{0}.zip".format(
                os.path.join(os.path.split(shapefile_base)[0], name)
            )

            with ZipFile(temp_archive, "w") as zfile:
                for extension, filepath in shapefile_plus_sidecars.items():
                    filename = "{0}.{1}".format(name, extension)
                    zfile.write(filename=filepath, arcname=filename)

            files = {"file": open(temp_archive, "rb")}

        # Shapefile Zip Case
        elif shapefile_zip:
            if is_zipfile(shapefile_zip):
                files = {"file": open(shapefile_zip, "rb")}
            else:
                raise TypeError('"{0}" is not a zip archive.'.format(shapefile_zip))

        # Shapefile Upload Case
        elif shapefile_upload:
            # Write files in memory to zipfile in memory
            zip_file_in_memory = BytesIO()

            with ZipFile(zip_file_in_memory, "w") as zfile:
                for file in shapefile_upload:
                    extension = os.path.splitext(file.name)[1]
                    filename = "{0}{1}".format(name, extension)
                    zfile.writestr(filename, file.read())

            files = {"file": zip_file_in_memory.getvalue()}

        # Prepare headers
        headers = {"Content-type": "application/zip", "Accept": "application/xml"}

        # Prepare URL
        url = self._assemble_url(
            "workspaces", workspace, "datastores", name, "file.shp"
        )

        # Set params
        params = {}

        if charset:
            params["charset"] = charset

        if overwrite:
            params["update"] = "overwrite"

        # Execute: PUT /workspaces/<ws>/datastores/<ds>/file.shp
        response = requests.put(
            url=url,
            files=files,
            headers=headers,
            params=params,
            auth=HTTPBasicAuth(username=self.username, password=self.password),
        )

        # Clean up file stuff
        if shapefile_base or shapefile_zip:
            files["file"].close()

        if temp_archive:
            os.remove(temp_archive)

        if zip_file_in_memory:
            zip_file_in_memory.close()

        # Wrap up with failure
        if response.status_code != 201:
            response_dict = {
                'success': False,
                'error': f'{response.reason}({response.status_code}): {response.text}'
            }

            self._handle_debug(response_dict, debug)
            return response_dict

        # Set the default style
        if default_style is not None:
            layer_url = self._assemble_url("layers", name)
            layer_headers = {"Content-Type": "application/xml"}
            layer_data = f"""
                <layer>
                <defaultStyle>
                    <name>{default_style}</name>
                </defaultStyle>
                </layer>
                """

            layer_response = requests.put(
                layer_url,
                headers=layer_headers,
                data=layer_data,
                auth=HTTPBasicAuth(username=self.username, password=self.password)
            )

            if layer_response.status_code != 200:
                layer_response_dict = {
                    'success': False,
                    'error': f'{layer_response.reason}({layer_response.status_code}): {layer_response.text}'
                }

                self._handle_debug(layer_response_dict, debug)
                return layer_response_dict

        # Wrap up successfully
        new_resource = self.catalog.get_resource(
            store=name, workspace=workspace
        )
        resource_dict = self._transcribe_geoserver_object(new_resource)

        response_dict = {"success": True, "result": resource_dict}
        self._handle_debug(response_dict, debug)
        return response_dict

    def create_coverage_store(self, store_id, coverage_type, debug=False):
        """
        Create a new coverage store.

        Args:
            store_id (string): Identifier for the store to be created. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
            coverage_type (str): Type of coverage store to create (e.g.: GeoServerAPI.CT_ARC_GRID, GeoServerAPI.CT_GEOTIFF, GeoServerAPI.CT_GRASS_GRID).
            debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
        """  # noqa: E501
        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Validate coverage type
        if coverage_type not in self.VALID_COVERAGE_TYPES:
            raise ValueError(
                '"{0}" is not a valid coverage_type. Use either {1}'.format(
                    coverage_type, ", ".join(self.VALID_COVERAGE_TYPES)
                )
            )

        # Black magic for grass grid support
        if coverage_type == self.CT_GRASS_GRID:
            coverage_type = self.CT_ARC_GRID

        # create the store
        xml = """
              <coverageStore>
                  <name>{name}</name>
                  <type>{type}</type>
                  <enabled>true</enabled>
                  <workspace>
                      <name>{workspace}</name>
                  </workspace>
              </coverageStore>
              """.format(
            name=name, type=coverage_type, workspace=workspace
        )

        # Prepare headers
        headers = {"Content-type": "text/xml", "Accept": "application/xml"}

        # Prepare URL to create store
        url = self._assemble_url("workspaces", workspace, "coveragestores")

        # Execute: POST /workspaces/<ws>/coveragestores
        response = requests.post(
            url=url, data=xml, headers=headers, auth=(self.username, self.password)
        )

        # Return with error if this doesn't work
        if response.status_code != 201:
            msg = "Create Coverage Store Status Code {0}: {1}".format(
                response.status_code, response.text
            )
            exception = requests.RequestException(msg, response=response)
            log.error(exception)
            raise exception

        # Wrap up successfully with new store created
        response_dict = self.get_store(store_id, debug)

        return response_dict

    def create_coverage_layer(
        self,
        layer_id,
        coverage_type,
        coverage_file,
        default_style="",
        other_styles=None,
        debug=False,
    ):
        """
        Create a coverage store, coverage resource, and layer in the given workspace.

        Args:
            layer_id (string): Identifier of the coverage layer to be created. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
            coverage_type (str): Type of coverage store to create (e.g.: GeoServerAPI.CT_ARC_GRID, GeoServerAPI.CT_GEOTIFF, GeoServerAPI.CT_GRASS_GRID).
            coverage_file (str): Path to coverage file or zip archive containing coverage file.
            default_style (str, optional): The name of the default style (note: it is assumed this style belongs to the workspace).
            other_styles (list, optional): A list of other default style names (assumption: these styles belong to the workspace).
            debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
        """  # noqa: E501
        # Process identifier
        workspace, coverage_name = self._process_identifier(layer_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Validate coverage type
        if coverage_type not in self.VALID_COVERAGE_TYPES:
            exception = ValueError(
                '"{0}" is not a valid coverage_type. Use either {1}'.format(
                    coverage_type, ", ".join(self.VALID_COVERAGE_TYPES)
                )
            )
            log.error(exception)
            raise exception

        # Only one coverage per coverage store, so we name coverage store the same as the coverage
        coverage_store_name = coverage_name

        # Prepare files
        working_dir = tempfile.mkdtemp()

        # Unzip to working directory if zip file
        if is_zipfile(coverage_file):
            zip_file = ZipFile(coverage_file)
            zip_file.extractall(working_dir)
        # Otherwise, copy to working directory
        else:
            shutil.copy2(coverage_file, working_dir)

        # Convert GrassGrids to ArcGrids
        if coverage_type == self.CT_GRASS_GRID:
            working_dir_contents = os.listdir(working_dir)
            num_working_dir_items = len(working_dir_contents)
            if num_working_dir_items > 2:
                exception = ValueError(
                    'Expected 1 or 2 files for coverage type "{}" but got {} instead: "{}"'.format(
                        self.CT_GRASS_GRID,
                        num_working_dir_items,
                        '", "'.join(working_dir_contents),
                    )
                )
                log.error(exception)
                raise exception

            for item in working_dir_contents:
                # Skip directories
                if os.path.isdir(os.path.join(working_dir, item)):
                    continue

                # Skip the projection file
                if "prj" in item:
                    continue

                # Assume other file is the raster
                corrupt_file = False
                tmp_coverage_path = os.path.join(working_dir, item)

                with open(tmp_coverage_path, "r") as item:
                    contents = item.readlines()

                for line in contents[0:6]:
                    if "north" in line:
                        north = float(line.split(":")[1].strip())
                    elif "south" in line:
                        south = float(line.split(":")[1].strip())
                    elif "east" in line:
                        pass  # we don't use east in this algorithm so skip it.
                    elif "west" in line:
                        west = float(line.split(":")[1].strip())
                    elif "rows" in line:
                        rows = int(line.split(":")[1].strip())
                    elif "cols" in line:
                        cols = int(line.split(":")[1].strip())
                    else:
                        corrupt_file = True

                if corrupt_file:
                    exception = IOError(
                        "GRASS file could not be processed, check to ensure the GRASS grid is "
                        "correctly formatted or included."
                    )
                    log.error(exception)
                    raise exception

                # Calculate new header
                xllcorner = west
                yllcorner = south
                cellsize = (north - south) / rows

                header = [
                    "ncols         {0}\n".format(cols),
                    "nrows         {0}\n".format(rows),
                    "xllcorner     {0}\n".format(xllcorner),
                    "yllcorner     {0}\n".format(yllcorner),
                    "cellsize      {0}\n".format(cellsize),
                ]

                # Strip off old header and add new one
                for _ in range(0, 6):
                    contents.pop(0)
                contents = header + contents

                # Write the coverage to file
                with open(tmp_coverage_path, "w") as o:
                    for line in contents:
                        # Make sure the file ends with a new line
                        if line[-1] != "\n":
                            line = line + "\n"

                        o.write(line)

        # Prepare Files
        coverage_archive_name = coverage_name + ".zip"
        coverage_archive = os.path.join(working_dir, coverage_archive_name)
        with ZipFile(coverage_archive, "w") as zf:
            for item in os.listdir(working_dir):
                if item != coverage_archive_name:
                    zf.write(os.path.join(working_dir, item), item)

        files = {"file": open(coverage_archive, "rb")}
        content_type = "application/zip"

        # Prepare headers
        headers = {"Content-type": content_type, "Accept": "application/xml"}

        # Prepare URL
        extension = coverage_type.lower()

        if coverage_type == self.CT_GRASS_GRID:
            extension = self.CT_ARC_GRID.lower()

        url = self._assemble_url(
            "workspaces",
            workspace,
            "coveragestores",
            coverage_store_name,
            "file.{0}".format(extension),
        )

        # Set params
        params = {"coverageName": coverage_name}

        retries_remaining = 3
        zip_error_retries = 5
        raise_error = False

        while True:
            if coverage_type == self.CT_IMAGE_MOSAIC:
                # Image mosaic doesn't need params argument.
                response = requests.put(
                    url=url,
                    files=files,
                    headers=headers,
                    auth=(self.username, self.password),
                )
            else:
                response = requests.put(
                    url=url,
                    files=files,
                    headers=headers,
                    params=params,
                    auth=(self.username, self.password),
                )

            # Raise an exception if status code is not what we expect
            if response.status_code == 201:
                log.info("Successfully created coverage {}".format(coverage_name))
                break
            if response.status_code == 500 and "already exists" in response.text:
                log.warning("Coverage already exists {}".format(coverage_name))
                break
            if (
                response.status_code == 500
                and "Error occured unzipping file" in response.text
            ):
                zip_error_retries -= 1
                if zip_error_retries == 0:
                    raise_error = True
            else:
                retries_remaining -= 1
                if retries_remaining == 0:
                    raise_error = True

            if raise_error:
                msg = "Create Coverage Status Code {0}: {1}".format(
                    response.status_code, response.text
                )
                exception = requests.RequestException(msg, response=response)
                log.error(exception)
                raise exception

        # Clean up
        files["file"].close()

        if working_dir:
            shutil.rmtree(working_dir)

        if default_style:
            # Add styles to new layer
            self.update_layer_styles(
                layer_id=layer_id,
                default_style=default_style,
                other_styles=other_styles,
            )

        response_dict = self.get_layer(layer_id, coverage_store_name, debug)
        return response_dict

    def create_layer_group(self, layer_group_id, layers, styles, debug=False):
        """
        Create a layer group. The number of layers and the number of styles must be the same.

        Args:
          layer_group_id (string): Identifier of the layer group to create. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
          layers (iterable): A list of layer names to be added to the group. Must be the same length as the styles list.
          styles (iterable): A list of style names to  associate with each layer in the group. Must be the same length as the layers list.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          layers = ('layer1', 'layer2')

          styles = ('style1', 'style2')

          response = engine.create_layer_group(layer_group_id='layer_group_name', layers=layers, styles=styles)
        """  # noqa: E501
        # Process identifier
        workspace, group_name = self._process_identifier(layer_group_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        context = {"name": group_name, "layers": layers, "styles": styles}

        # Open layer group template
        template_path = os.path.join(self.XML_PATH, "layer_group_template.xml")
        url = self._assemble_url("workspaces", workspace, "layergroups.json")
        headers = {"Content-type": "text/xml"}

        with open(template_path, "r") as template_file:
            text = template_file.read()
            template = Template(text)
            xml = template.render(context)

        response = requests.post(
            url,
            headers=headers,
            auth=(self.username, self.password),
            data=xml,
        )

        if response.status_code != 201:
            msg = "Create Layer Group Status Code {}: {}".format(
                response.status_code, response.text
            )
            exception = requests.RequestException(msg, response=response)
            log.error(exception)
            raise exception

        response_dict = self.get_layer_group(layer_group_id, debug=debug)

        return response_dict

    def create_workspace(self, workspace_id, uri, debug=False):
        """
        Create a new workspace.

        Args:
          workspace_id (string): Identifier of the workspace to create. Must be unique.
          uri (string): URI associated with your project. Does not need to be a real web URL, just a unique identifier. One suggestion is to append the URL of your project with the name of the workspace (e.g.: http:www.example.com/workspace-name).  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.create_workspace(workspace_id='workspace_name', uri='www.example.com/workspace_name')
        """
        # Create workspace
        try:
            # Do create
            workspace = self.catalog.create_workspace(workspace_id, uri)
            workspace_dict = self._transcribe_geoserver_object(workspace)
            response_dict = {"success": True, "result": workspace_dict}

        except AssertionError as e:
            response_dict = {"success": False, "error": str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def create_style(
        self, style_id, sld_template, sld_context=None, overwrite=False, debug=False
    ):
        """
        Create style layer from an SLD template.

        Args
          style_id (string): Identifier of the style to create ('<workspace>:<name>').
          sld_template: path to SLD template file.
          sld_context: a dictionary with context variables to be rendered in the template.
          overwrite (bool, optional): Will overwrite existing style with same name if True. Defaults to False.
        """
        # Process identifier
        workspace, style_name = self._process_identifier(style_id)

        if workspace is None:
            url = self._assemble_url("styles")
        else:
            url = self._assemble_url("workspaces", workspace, "styles")

        if overwrite:
            try:
                self.delete_style(style_id, purge=True)
            except Exception as e:
                if "referenced by existing" in str(e):
                    log.error(str(e))
                    raise

        # Use post request to create style container first
        headers = {"Content-type": "application/vnd.ogc.sld+xml"}

        # Render the SLD template
        with open(sld_template, "r") as sld_file:
            text = sld_file.read()

        if sld_context is not None:
            template = Template(text)
            text = template.render(sld_context)

        response = requests.post(
            url,
            headers=headers,
            auth=(self.username, self.password),
            params={"name": style_name},
            data=text,
        )

        # Raise an exception if status code is not what we expect
        if response.status_code == 201:
            log.info("Successfully created style {}".format(style_name))
        else:
            msg = "Create Style Status Code {0}: {1}".format(
                response.status_code, response.text
            )
            if response.status_code == 500:
                if (
                    "Unable to find style for event" in response.text
                    or "Error persisting" in response.text
                ):
                    warning_msg = "Created style {} with warnings: {}".format(
                        style_name, response.text
                    )
                    log.warning(warning_msg)
                    return {"success": True, "result": warning_msg}
                else:
                    exception = requests.RequestException(msg, response=response)
                    log.error(msg)
                    raise exception
            else:
                exception = requests.RequestException(msg, response=response)
                log.error(msg)
                raise exception

        response_dict = self.get_style(style_id=style_id, debug=debug)
        return response_dict

    def update_resource(self, resource_id, store=None, debug=False, **kwargs):
        """
        Update an existing resource.

        Args:
          resource_id (string): Identifier of the resource to update. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          store (string, optional): Update a resource in this store.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Key value pairs representing the attributes and values to change.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.update_resource(resource_id='workspace:resource_name', enabled=False, title='New Title')
        """
        # Process identifier
        workspace, name = self._process_identifier(resource_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        try:
            # Get resource
            resource = self.catalog.get_resource(
                name=name, store=store, workspace=workspace
            )

            # Make the changes
            updated_resource = self._apply_changes_to_gs_object(kwargs, resource)

            # Save the changes
            self.catalog.save(updated_resource)

            # Return the updated resource dictionary
            resource_dict = self._transcribe_geoserver_object(updated_resource)

            # Assemble Response
            response_dict = {"success": True, "result": resource_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_layer(self, layer_id, debug=False, **kwargs):
        """
        Update an existing layer.

        Args:
          layer_id (string): Identifier of the layer to update. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Key value pairs representing the attributes and values to change.

        Returns:
          (dict): Response dictionary

        Examples:

          updated_layer = engine.update_layer(layer_id='workspace:layer_name', default_style='style1', styles=['style1', 'style2'])  # noqa: E501
        """
        # Pop tile caching properties to handle separately
        tile_caching = kwargs.pop("tile_caching", None)
        try:
            # Get resource
            layer = self.catalog.get_layer(name=layer_id)

            # Apply changes from kwargs
            updated_layer = self._apply_changes_to_gs_object(kwargs, layer)

            # Save the changes
            self.catalog.save(updated_layer)

            # Return the updated resource dictionary
            layer_dict = self._transcribe_geoserver_object(updated_layer)

            # Assemble Response
            response_dict = {'success': True,
                             'result': layer_dict}

            # Handle tile caching properties (gsconfig doesn't support this)
            if tile_caching is not None:
                gwc_url = "{0}layers/{1}.xml".format(self.gwc_endpoint, layer_id)
                auth = (self.username, self.password)
                xml = ConvertDictToXml({"GeoServerLayer": tile_caching})
                r = requests.post(
                    gwc_url,
                    auth=auth,
                    headers={"Content-Type": "text/xml"},
                    data=ElementTree.tostring(xml),
                )

                if r.status_code == 200:
                    layer_dict["tile_caching"] = tile_caching
                    response_dict = {"success": True, "result": layer_dict}
                else:
                    response_dict = {"success": False, "error": r.text}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_layer_group(self, layer_group_id, debug=False, **kwargs):
        """
        Update an existing layer. If modifying the layers, ensure the number of layers
        and the number of styles are the same.

        Args:
          layer_group_id (string): Identifier of the layer group to update.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Key value pairs representing the attributes and values to change

        Returns:
          (dict): Response dictionary

        Examples:

          updated_layer_group = engine.update_layer_group(layer_group_id='layer_group_name', layers=['layer1', 'layer2'], styles=['style1', 'style2'])  # noqa: E501
        """
        workspace, name = self._process_identifier(layer_group_id)

        try:
            # Get resource
            layer_group = self.catalog.get_layergroup(name=name, workspace=workspace)

            # Make the changes
            for attribute, value in kwargs.items():
                if hasattr(layer_group, attribute):
                    setattr(layer_group, attribute, value)

            # Save the changes
            self.catalog.save(layer_group)

            # Return the updated resource dictionary
            layer_group_dict = self._transcribe_geoserver_object(layer_group)

            # Assemble Response
            response_dict = {"success": True, "result": layer_group_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {"success": False, "error": str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_layer_styles(self, layer_id, default_style, other_styles=None, debug=False):
        """
        Update/add styles to existing layer.

        Args:
            layer_id (string): Identifier of the layer whose style will be update or added. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
            default_style (str): Name of default style. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name")
            other_styles (list<str>): Additional styles to add to layer. List elements can be names or workspace-name combinations (e.g.: "name" or "workspace:name")
            debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
        """  # noqa: E501
        # Process identifier
        layer_workspace, layer_name = self._process_identifier(layer_id)

        if not layer_workspace:
            layer_workspace = self.catalog.get_default_workspace().name

        # check if layer workspace is style workspace else use styles default location
        lyr_ws_styles = self.list_styles(workspace=layer_workspace)
        if default_style in lyr_ws_styles:
            default_style = '{0}:{1}'.format(layer_workspace, default_style)
        if other_styles:
            for i in range(len(other_styles)):
                if other_styles[i] in lyr_ws_styles:
                    other_styles[i] = '{0}:{1}'.format(layer_workspace, other_styles[i])

        context = {
            'default_style': default_style,
            'other_styles': other_styles or [],
            'geoserver_rest_endpoint': self.endpoint
        }

        # Open layer template
        layer_path = os.path.join(self.XML_PATH, 'layer_template.xml')
        url = self._assemble_url('layers', '{0}.xml'.format(layer_name))
        headers = {
            "Content-type": "text/xml"
        }

        with open(layer_path, 'r') as layer_file:
            text = layer_file.read()
            template = Template(text)
            xml = template.render(context)

        retries_remaining = 3
        while retries_remaining > 0:
            response = requests.put(
                url,
                headers=headers,
                auth=(self.username, self.password),
                data=xml,
            )

            # Raise an exception if status code is not what we expect
            if response.status_code == 200:
                log.info('Successfully created layer {}'.format(layer_name))
                break
            else:
                retries_remaining -= 1
                if retries_remaining == 0:
                    msg = "Create Layer Status Code {0}: {1}".format(response.status_code, response.text)
                    exception = requests.RequestException(msg, response=response)
                    log.error(exception)
                    raise exception

        response_dict = self.get_layer(layer_id=layer_id, debug=debug)

        return response_dict

    def delete_resource(
        self, resource_id, store_id, purge=False, recurse=False, debug=False
    ):
        """
        Delete a resource.

        Args:
          resource_id (string): Identifier of the resource to delete. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").  # noqa: E501
          store_id (string): Return only resources belonging to a certain store.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively any dependencies if True (i.e.: layers or layer groups it belongs to).  # noqa: E501
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.delete_resource('workspace:resource_name')
        """
        workspace, name = self._process_identifier(resource_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Get resource
        resource = self.catalog.get_resource(
            name=name, store=store_id, workspace=workspace
        )

        # Handle delete
        return self._handle_delete(
            identifier=name,
            gs_object=resource,
            purge=purge,
            recurse=recurse,
            debug=debug,
        )

    def delete_layer(self, layer_id, datastore, recurse=False):
        """
        Args:
            layer_id (string): Identifier of the layer to delete. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
            datastore: Name of datastore
            recurse (bool): recursively delete any dependent objects if True.
        """  # noqa: E501
        # Process identifier
        workspace, name = self._process_identifier(layer_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        url = self._assemble_url(
            "workspaces", workspace, "datastores", datastore, "featuretypes", name
        )
        # Prepare delete request
        headers = {"Content-type": "application/json"}

        json = {"recurse": recurse}

        response = requests.delete(
            url, auth=(self.username, self.password), headers=headers, params=json
        )

        # Raise an exception if status code is not what we expect
        if response.status_code != 200:
            if response.status_code in self.WARNING_STATUS_CODES:
                pass
            else:
                msg = "Delete Layer Status Code {0}: {1}".format(
                    response.status_code, response.text
                )
                exception = requests.RequestException(msg, response=response)
                log.error(exception)
                raise exception

        response_dict = {"success": True, "result": None}
        return response_dict

    def delete_layer_group(self, layer_group_id):
        """
        Delete the specified layer-group.  Works around a GeoServer 500 / NPE
        that occurs on workspace-qualified groups by always passing
        ``recurse=true``.
        """
        # Process identifier
        workspace, group_name = self._process_identifier(layer_group_id)

        # Fall back to default workspace
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        url = self._assemble_url("workspaces", workspace, "layergroups", f"{group_name}")
        response = requests.delete(
            url,
            auth=(self.username, self.password),
            params={"recurse": "true"},
        )

        if response.status_code != 200:
            if response.status_code == 404 and "No such layer group" in response.text:
                pass
            else:
                msg = "Delete Layer Group Status Code {0}: {1}".format(response.status_code, response.text)
                exception = requests.RequestException(msg, response=response)
                log.error(exception)
                raise exception

        response_dict = {'success': True, 'result': None}
        return response_dict

    def delete_workspace(self, workspace_id, purge=False, recurse=False, debug=False):
        """
        Delete a workspace.

        Args:
          workspace_id (string): Identifier of the workspace to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.delete_resource('workspace_name')
        """
        # Get layer group
        workspace = self.catalog.get_workspace(workspace_id)

        # Handle delete
        return self._handle_delete(
            identifier=workspace_id,
            gs_object=workspace,
            purge=purge,
            recurse=recurse,
            debug=debug,
        )

    def delete_store(self, store_id, purge=False, recurse=False, debug=False):
        """
        Delete a store.

        Args:
          store_id (string): Identifier of the store to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.delete_store('workspace:store_name')
        """
        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Get layer group
        try:
            store = self.catalog.get_store(name=name, workspace=workspace)

            # Handle delete
            return self._handle_delete(
                identifier=store_id,
                gs_object=store,
                purge=purge,
                recurse=recurse,
                debug=debug,
            )
        except geoserver.catalog.FailedRequestError as e:
            # Update response dictionary
            response_dict = {"success": False, "error": str(e)}

            self._handle_debug(response_dict, debug)
            return response_dict

    def delete_coverage_store(self, store_id, recurse=True, purge=True):
        """
        Delete the specified coverage store.

        Args:
            store_id (string): Identifier for the store to be deleted. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
            recurse (bool): recursively delete any dependent objects if True.
            purge (bool): delete configuration files from filesystem if True. remove file from disk of geoserver.
            debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:

        """  # noqa: E501
        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        # Prepare headers
        headers = {"Content-type": "application/json"}

        # Prepare URL to create store
        url = self._assemble_url("workspaces", workspace, "coveragestores", name)

        json = {"recurse": recurse, "purge": purge}

        # Execute: DELETE /workspaces/<ws>/coveragestores/<cs>
        response = requests.delete(
            url=url, headers=headers, params=json, auth=(self.username, self.password)
        )

        if response.status_code != 200:
            if response.status_code in self.WARNING_STATUS_CODES:
                pass
            else:
                msg = "Delete Coverage Store Status Code {0}: {1}".format(
                    response.status_code, response.text
                )
                exception = requests.RequestException(msg, response=response)
                log.error(exception)
                raise exception

        response_dict = {"success": True, "result": None}
        return response_dict

    def delete_style(self, style_id, purge=False):
        """
        Delete the style with the given workspace and style name.

        Args:
            style_id (string): Identifier of the style to delete. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name").
            purge (bool): delete configuration files from filesystem if True. remove file from disk of geoserver.

        Returns:
        """  # noqa: E501
        # Process identifier
        workspace, style_name = self._process_identifier(style_id)

        if workspace is None:
            url = self._assemble_url("styles", style_name)
        else:
            url = self._assemble_url("workspaces", workspace, "styles", style_name)

        # Prepare delete request
        headers = {"Content-type": "application/json"}

        params = {"purge": purge}

        response = requests.delete(
            url=url, auth=(self.username, self.password), headers=headers, params=params
        )

        # Raise an exception if status code is not what we expect
        if response.status_code != 200:
            if response.status_code in self.WARNING_STATUS_CODES:
                pass
            else:
                msg = "Delete Style Status Code {0}: {1}".format(
                    response.status_code, response.text
                )
                exception = requests.RequestException(msg, response=response)
                log.error(exception)
                raise exception

        response_dict = {"success": True, "result": None}
        return response_dict

    def validate(self):
        """
        Validate the GeoServer spatial dataset engine. Will throw and error if not valid.
        """
        try:
            r = requests.get(self.endpoint, auth=(self.username, self.password))

        except requests.exceptions.MissingSchema:
            raise AssertionError(
                'The URL "{0}" provided for the GeoServer spatial dataset service endpoint is '
                "invalid.".format(self.endpoint)
            )

        if r.status_code == 401:
            raise AssertionError(
                "The username and password of the GeoServer spatial dataset service engine are "
                "not valid."
            )

        if r.status_code != 200:
            raise AssertionError(
                'The URL "{0}" is not a valid GeoServer spatial dataset service '
                "endpoint.".format(self.endpoint)
            )

        if "Geoserver Configuration API" not in r.text:
            raise AssertionError(
                'The URL "{0}" is not a valid GeoServer spatial dataset service '
                "endpoint.".format(self.endpoint)
            )

    def modify_tile_cache(
        self,
        layer_id,
        operation,
        zoom_start=10,
        zoom_end=15,
        grid_set_id=900913,
        image_format="image/png",
        thread_count=1,
        bounds=None,
        parameters=None,
    ):
        """
        Modify all or a portion of the GWC tile cache for given layer. Operations include seed, reseed, and truncate.

        Args:
            layer_id (string): Identifier of the layer. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
            operation (str): operation type either 'seed', 'reseed', 'truncate', or 'masstruncate'.
            zoom_start (int, optional): beginning of zoom range on which to perform tile cache operation. Minimum is 0. Defaults to 10.
            zoom_end (int, optional): end of zoom range on which to perform tile cache operation. It is not usually recommended to seed past zoom 20. Maximum is 30. Defaults to 15.
            grid_set_id (int, optional): ID of the grid set on which to perform the tile cache operation. Either 4326 for Geographic or 900913 for Web Mercator. Defaults to 900913.
            image_format (str, optional): format of tiles on which to perform tile cache operation. Defaults to 'image/png'.
            thread_count (int, optional): number of threads to used to perform tile cache operation. Defaults to 1.
            bounds (list, optional): list with ordinates of bounding box of area on which to perform tile cache operation (e.g.: [minx, miny, maxx, maxy]).
            parameters (dict, optional): Key value pairs of parameters to use to filter tile cache operation.

        Raises:
            requests.RequestException: if modify tile cache operation is not submitted successfully.
            ValueError: if invalid value is provided for an argument.
        """  # noqa: E501
        # Process identifier
        workspace, name = self._process_identifier(layer_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        if operation not in self.GWC_OPERATIONS:
            raise ValueError(
                'Invalid value "{}" provided for argument "operation". Must be "{}".'.format(
                    operation, '" or "'.join(self.GWC_OPERATIONS)
                )
            )

        # Use post request to create style container first
        headers = {"Content-type": "text/xml"}

        if operation == self.GWC_OP_MASS_TRUNCATE:
            url = self.get_gwc_endpoint() + "masstruncate/"
            xml_text = (
                "<truncateLayer><layerName>{}:{}</layerName></truncateLayer>".format(
                    workspace, name
                )
            )

            response = requests.post(
                url, headers=headers, auth=(self.username, self.password), data=xml_text
            )

        else:
            url = self.get_gwc_endpoint() + "seed/" + workspace + ":" + name + ".xml"
            xml = os.path.join(self.XML_PATH, "gwc_tile_cache_operation_template.xml")

            # Open XML file
            with open(xml, "r") as sld_file:
                text = sld_file.read()

            # Compose XML context
            xml_context = {
                "workspace": workspace,
                "name": name,
                "operation": operation,
                "grid_set_id": grid_set_id,
                "zoom_start": zoom_start,
                "zoom_end": zoom_end,
                "format": image_format,
                "thread_count": thread_count,
                "parameters": parameters,
                "bounds": bounds,
            }

            # Render the XML template
            template = Template(text)
            rendered = template.render(xml_context)

            response = requests.post(
                url, headers=headers, auth=(self.username, self.password), data=rendered
            )

        # Raise an exception if status code is not what we expect
        if response.status_code == 200:
            log.info(
                "Successfully submitted {} tile cache operation for layer {}:{}".format(
                    operation, workspace, name
                )
            )
        else:
            msg = "Unable to submit {} tile cache operation for layer {}:{}. {}:{}".format(
                operation, workspace, name, response.status_code, response.text
            )
            exception = requests.RequestException(msg, response=response)
            log.error(msg)
            raise exception

        response_dict = {"success": True, "result": None}
        return response_dict

    def terminate_tile_cache_tasks(self, layer_id, kill="all"):
        """
        Terminate running tile cache processes for given layer.

        Args:
            layer_id (string): Identifier of the layer. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
            kill (str): specify which type of task to terminate. Either 'running', 'pending', or 'all'.

        Raises:
            requests.RequestException: if terminate tile cache operation cannot be submitted successfully.
            ValueError: if invalid value is provided for an argument.
        """  # noqa: E501
        # Process identifier
        workspace, name = self._process_identifier(layer_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        if kill not in self.GWC_KILL_OPERATIONS:
            raise ValueError(
                'Invalid value "{}" provided for argument "kill". Must be "{}".'.format(
                    kill, '" or "'.join(self.GWC_KILL_OPERATIONS)
                )
            )

        url = self.get_gwc_endpoint() + "seed/" + workspace + ":" + name

        response = requests.post(
            url, auth=(self.username, self.password), data={"kill_all": kill}
        )

        if response.status_code != 200:
            msg = "Unable to query tile cache status for layer {}:{}. {}:{}".format(
                workspace, name, response.status_code, response.text
            )
            exception = requests.RequestException(msg, response=response)
            raise exception

        response_dict = {"success": True, "result": None}
        return response_dict

    def query_tile_cache_tasks(self, layer_id):
        """
        Get the status of running tile cache tasks for a layer.

        Args:
            layer_id (string): Identifier of the layer. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").

        Returns:
            list: list of dictionaries with status with keys: 'tiles_processed', 'total_to_process', 'num_remaining', 'task_id', 'task_status'

        Raises:
            requests.RequestException: if query tile cache operation cannot be submitted successfully.
        """  # noqa: E501
        # Process identifier
        workspace, name = self._process_identifier(layer_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        url = self.get_gwc_endpoint() + "seed/" + workspace + ":" + name + ".json"
        status_list = []

        response = requests.get(
            url,
            auth=(self.username, self.password),
        )

        if response.status_code == 200:
            status = response.json()

            if "long-array-array" in status:
                for s in status["long-array-array"]:
                    temp_dict = {
                        "tiles_processed": s[0],
                        "total_to_process": s[1],
                        "num_remaining": s[2],
                        "task_id": s[3],
                        "task_status": (
                            self.GWC_STATUS_MAP[s[4]]
                            if s[4] in self.GWC_STATUS_MAP
                            else s[4]
                        ),
                    }

                    status_list.append(dict(temp_dict))
            return status_list
        else:
            msg = "Unable to terminate tile cache tasks for layer {}:{}. {}:{}".format(
                workspace, name, response.status_code, response.text
            )
            exception = requests.RequestException(msg, response=response)
            raise exception

    def enable_time_dimension(self, coverage_id):
        """
        Enable time dimension for a given image mosaic layer

        Args:
            coverage_id (str): name of the image mosaic layer including workspace. (e.g: workspace:name).

        Raises:
            requests.RequestException: if enable time dimension operation cannot be executed successfully.
        """  # noqa: E501
        # Process identifier
        workspace, coverage_name = self._process_identifier(coverage_id)

        # Get default work space if none is given
        if not workspace:
            workspace = self.catalog.get_default_workspace().name

        headers = {"Content-type": "text/xml"}
        url = self._assemble_url(
            "workspaces",
            workspace,
            "coveragestores",
            coverage_name,
            "coverages",
            coverage_name,
        )
        data_xml = '<coverage>\
                    <enabled>true</enabled>\
                    <metadata><entry key="time">\
                    <dimensionInfo>\
                    <enabled>true</enabled>\
                    <presentation>LIST</presentation>\
                    <units>ISO8601</units><defaultValue/>\
                    </dimensionInfo>\
                    </entry></metadata>\
                    </coverage>'
        response = requests.put(
            url,
            headers=headers,
            auth=(self.username, self.password),
            data=data_xml,
        )

        if response.status_code != 200:
            msg = (
                f"Enable Time Dimension Layer {coverage_name} with Status Code {response.status_code}:"
                f" {response.text}"
            )
            exception = requests.RequestException(msg, response=response)
            log.error(exception)
            raise exception

        response_dict = {"success": True, "result": None}
        return response_dict
