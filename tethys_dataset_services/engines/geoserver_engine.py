import os
import pprint
import requests
from io import BytesIO
from xml.etree import ElementTree
from zipfile import ZipFile, is_zipfile

from past.builtins import basestring
from requests.auth import HTTPBasicAuth
import geoserver
from geoserver.catalog import Catalog as GeoServerCatalog
from geoserver.support import JDBCVirtualTable, JDBCVirtualTableGeometry, JDBCVirtualTableParam
from geoserver.util import shapefile_and_friends

from ..utilities import ConvertDictToXml, ConvertXmlToDict
from ..base import SpatialDatasetEngine


class GeoServerSpatialDatasetEngine(SpatialDatasetEngine):
    """
    Definition for GeoServer Dataset Engine objects.
    """
    @property
    def type(self):
        """
        GeoServer Spatial Dataset Type
        """
        return 'GEOSERVER'

    @property
    def gwc_endpoint(self):
        return self._gwc_endpoint

    def __init__(self, endpoint, apikey=None, username=None, password=None):
        """
        Default constructor for Dataset Engines.

        Args:
          api_endpoint (string): URL of the dataset service API endpoint (e.g.: www.host.com/api)
          apikey (string, optional): API key that will be used to authenticate with the dataset service.
          username (string, optional): Username that will be used to authenticate with the dataset service.
          password (string, optional): Password that will be used to authenticate with the dataset service.
        """
        # Set custom property /geoserver/rest/ -> /geoserver/gwc/rest/
        if '/' == endpoint[-1]:
            self._gwc_endpoint = endpoint.replace('rest', 'gwc/rest')
        else:
            self._gwc_endpoint = endpoint.replace('rest', 'gwc/rest/')

        super(GeoServerSpatialDatasetEngine, self).__init__(
            endpoint=endpoint,
            apikey=apikey,
            username=username,
            password=password
        )

    def _apply_changes_to_gs_object(self, attributes_dict, gs_object):
        # Catalog object
        catalog = self._get_geoserver_catalog_object()

        # Make the changes
        for attribute, value in attributes_dict.iteritems():
            if hasattr(gs_object, attribute):
                if attribute == 'styles':
                    styles_objects = []

                    for style in attributes_dict['styles']:
                        # Lookup by name and workspace
                        if ':' in style:
                            style_split = style.split(':')
                            styles_objects.append(catalog.get_style(name=style_split[1], workspace=style_split[0]))
                        # Lookup by name only
                        else:
                            styles_objects.append(catalog.get_style(name=style))

                    setattr(gs_object, 'styles', styles_objects)

                elif attribute == 'default_style':
                    style = attributes_dict['default_style']

                    if ':' in style:
                        style_split = style.split(':')
                        style_object = catalog.get_style(name=style_split[1], workspace=style_split[0])

                    # Lookup by name only
                    else:
                        style_object = catalog.get_style(name=style)

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
        if endpoint[-1] == '/':
            endpoint = endpoint[:-1]

        pieces = list(args)
        pieces.insert(0, endpoint)
        return '/'.join(pieces)

    def _get_non_rest_endpoint(self):
        """
        Get endpoint without the "rest".
        """
        endpoint = self.endpoint

        # Eliminate trailing slash if necessary
        if endpoint[-1] == '/':
            endpoint = endpoint[:-1]
        if endpoint[-5:] == '/rest':
            endpoint = endpoint[:-5]
        return endpoint

    def _get_geoserver_catalog_object(self):
        """
        Internal method used to get the connection object to GeoServer.
        """
        return GeoServerCatalog(self.endpoint, username=self.username, password=self.password)

    def _get_wms_url(self, layer_id, style='', srs='EPSG:4326', bbox='-180,-90,180,90', version='1.1.0',
                     width='512', height='512', output_format='image/png', tiled=False, transparent=True):
        """
        Assemble a WMS url.
        """
        endpoint = self._get_non_rest_endpoint()

        if tiled:
            tiled_option='yes'
        else:
            tiled_option='no'

        if transparent:
            transparent_option='true'
        else:
            transparent_option='false'

        wms_url = '{0}/wms?service=WMS&version={1}&request=GetMap&' \
                  'layers={2}&styles={3}&' \
                  'transparent={10}&tiled={9}&' \
                  'srs={4}&bbox={5}&' \
                  'width={6}&height={7}&' \
                  'format={8}'.format(endpoint, version, layer_id, style, srs, bbox, width, height, output_format,
                                      tiled_option, transparent_option)

        return wms_url

    def _get_wcs_url(self, resource_id, srs='EPSG:4326', bbox='-180,-90,180,90', output_format='png', namespace=None,
                     width='512', height='512'):
        """
        Assemble a WCS url.
        """
        endpoint = self._get_non_rest_endpoint()

        wcs_url = '{0}/wcs?service=WCS&version=1.1.0&request=GetCoverage&' \
                  'identifier={1}&' \
                  'srs={2}&BoundingBox={3}&' \
                  'width={5}&height={6}&' \
                  'format={4}'.format(endpoint, resource_id, srs, bbox, output_format, width, height)

        if namespace and isinstance(namespace, basestring):
            wcs_url = '{0}&namespace={1}'.format(wcs_url, namespace)

        return wcs_url

    def _get_wfs_url(self, resource_id, output_format='GML3'):
        """
        Assemble a WFS url.
        """
        endpoint = self._get_non_rest_endpoint()

        if output_format == 'GML3':
            wfs_url = '{0}/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames={1}'.format(endpoint, resource_id)
        elif output_format == 'GML2':
            wfs_url = '{0}/wfs?service=WFS&version=1.0.0&request=GetFeature&typeNames={1}&' \
                      'outputFormat=GML2'.format(endpoint, resource_id)
        else:
            wfs_url = '{0}/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames={1}&' \
                      'outputFormat={2}'.format(endpoint, resource_id, output_format)

        return wfs_url

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
        # Get a GeoServer catalog object and query for list of resources
        catalog = self._get_geoserver_catalog_object()

        # Initialize response dictionary
        response_dict = {'success': False}
        if gs_object:
            try:
                # Execute
                catalog.delete(config_object=gs_object, purge=purge, recurse=recurse)

                # Update response dictionary
                response_dict['success'] = True
                response_dict['result'] = None

            except geoserver.catalog.FailedRequestError as e:
                # Update response dictionary
                response_dict['success'] = False
                response_dict['error'] = str(e)

        else:
            # Update response dictionary
            response_dict['success'] = False
            response_dict['error'] = 'GeoServer object does not exist: "{0}".'.format(identifier)

        # Refresh the catalog
        catalog.reload()

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
            response_dict = {'success': True,
                             'result': names}

            # Handle the debug and return
            self._handle_debug(response_dict, debug)
            return response_dict

        # Handle the debug and return
        gs_object_dicts = self._transcribe_geoserver_objects(gs_objects)

        # Assemble Response
        response_dict = {'success': True,
                         'result': gs_object_dicts}

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
        if ':' in identifier:
            workspace, name = identifier.split(':')

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
        NAMED_OBJECTS = ('store', 'workspace')
        NAMED_OBJECTS_WITH_WORKSPACE = ('resource', 'default_style')
        OMIT_ATTRIBUTES = ('writers', 'attribution_object', 'dirty', 'dom', 'save_method')

        # Load into a dictionary
        object_dictionary = {}
        resource_object = None

        # Get the non-private attributes
        attributes = [a for a in dir(gs_object) if not a.startswith('__') and not a.startswith('_')]

        for attribute in attributes:
            if not callable(getattr(gs_object, attribute)):
                # Handle special cases upfront
                if attribute in NAMED_OBJECTS:
                    sub_object = getattr(gs_object, attribute)
                    if not sub_object or isinstance(sub_object, basestring):
                        object_dictionary[attribute] = sub_object
                    else:
                        object_dictionary[attribute] = sub_object.name

                elif attribute in NAMED_OBJECTS_WITH_WORKSPACE:
                    # Append workspace if applicable
                    sub_object = getattr(gs_object, attribute)

                    # Stash resource for later use
                    if attribute == 'resource':
                        resource_object = sub_object

                    if sub_object and not isinstance(sub_object, str):
                        if sub_object.workspace:
                            try:
                                object_dictionary[attribute] = '{0}:{1}'.format(sub_object.workspace.name, sub_object.name)
                            except AttributeError:
                                object_dictionary[attribute] = '{0}:{1}'.format(sub_object.workspace, sub_object.name)
                        else:
                            object_dictionary[attribute] = sub_object.name
                    elif isinstance(sub_object, str):
                        object_dictionary[attribute] = getattr(gs_object, attribute)

                elif attribute in OMIT_ATTRIBUTES:
                    # Omit these attributes
                    pass

                elif attribute == 'catalog':
                    # Store URL in place of catalog
                    catalog_object = getattr(gs_object, 'catalog')
                    object_dictionary[attribute] = catalog_object.gs_base_url

                elif attribute == 'styles':
                    styles = getattr(gs_object, attribute)
                    styles_names = []
                    for style in styles:
                        if style is not None:
                            if not isinstance(style, str):
                                if style.workspace:
                                    styles_names.append('{0}:{1}'.format(style.workspace, style.name))
                                else:
                                    styles_names.append(style.name)
                            else:
                                object_dictionary[attribute] = getattr(gs_object, attribute)

                    object_dictionary[attribute] = styles_names

                # Store attribute properties as is
                else:
                    object_dictionary[attribute] = getattr(gs_object, attribute)

        # Inject appropriate WFS and WMS URLs
        if 'resource_type' in object_dictionary:
            # Feature Types Get WFS
            if object_dictionary['resource_type'] == 'featureType':
                if object_dictionary['workspace']:
                    resource_id = '{0}:{1}'.format(object_dictionary['workspace'], object_dictionary['name'])
                else:
                    resource_id = object_dictionary['name']

                object_dictionary['wfs'] = {
                    'gml3': self._get_wfs_url(resource_id, 'GML3'),
                    'gml2': self._get_wfs_url(resource_id, 'GML2'),
                    'shapefile': self._get_wfs_url(resource_id, 'shape-zip'),
                    'geojson': self._get_wfs_url(resource_id, 'application/json'),
                    'geojsonp': self._get_wfs_url(resource_id, 'text/javascript'),
                    'csv': self._get_wfs_url(resource_id, 'csv')
                }

            # Coverage Types Get WCS
            elif object_dictionary['resource_type'] == 'coverage':
                workspace = None
                name = object_dictionary['name']
                bbox = '-180,-90,180,90'
                srs = 'EPSG:4326'
                width = '512'
                height = '512'

                if object_dictionary['workspace']:
                    workspace = object_dictionary['workspace']

                if resource_object and resource_object.native_bbox:
                    # Find the native bounding box
                    nbbox = resource_object.native_bbox
                    minx = nbbox[0]
                    maxx = nbbox[1]
                    miny = nbbox[2]
                    maxy = nbbox[3]
                    srs = resource_object.projection
                    bbox = '{0},{1},{2},{3}'.format(minx, miny, maxx, maxy)

                    # Resize the width to be proportionate to the image aspect ratio
                    aspect_ratio = (float(maxx) - float(minx)) / (float(maxy) - float(miny))
                    width = str(int(aspect_ratio * float(height)))

                object_dictionary['wcs'] = {
                    'png': self._get_wcs_url(name, output_format='png', namespace=workspace, srs=srs, bbox=bbox),
                    'gif': self._get_wcs_url(name, output_format='gif', namespace=workspace, srs=srs, bbox=bbox),
                    'jpeg': self._get_wcs_url(name, output_format='jpeg', namespace=workspace, srs=srs, bbox=bbox),
                    'tiff': self._get_wcs_url(name, output_format='tif', namespace=workspace, srs=srs, bbox=bbox),
                    'bmp': self._get_wcs_url(name, output_format='bmp', namespace=workspace, srs=srs, bbox=bbox),
                    'geotiff': self._get_wcs_url(name, output_format='geotiff', namespace=workspace, srs=srs, bbox=bbox),
                    'gtopo30': self._get_wcs_url(name, output_format='gtopo30', namespace=workspace, srs=srs, bbox=bbox),
                    'arcgrid': self._get_wcs_url(name, output_format='ArcGrid', namespace=workspace, srs=srs, bbox=bbox),
                    'arcgrid_gz': self._get_wcs_url(name, output_format='ArcGrid-GZIP', namespace=workspace, srs=srs, bbox=bbox),
                }

            elif object_dictionary['resource_type'] == 'layer':
                # Defaults
                bbox = '-180,-90,180,90'
                srs = 'EPSG:4326'
                width = '512'
                height = '512'
                style = ''

                # Layer and style
                layer = object_dictionary['name']
                if 'default_style' in object_dictionary:
                    style = object_dictionary['default_style']

                # Try to extract the bounding box from the resource which was saved earlier
                if resource_object and resource_object.native_bbox:
                    # Find the native bounding box
                    nbbox = resource_object.native_bbox
                    minx = nbbox[0]
                    maxx = nbbox[1]
                    miny = nbbox[2]
                    maxy = nbbox[3]
                    srs = resource_object.projection
                    bbox = '{0},{1},{2},{3}'.format(minx, miny, maxx, maxy)

                    # Resize the width to be proportionate to the image aspect ratio
                    aspect_ratio = (float(maxx) - float(minx)) / (float(maxy) - float(miny))
                    width = str(int(aspect_ratio * float(height)))

                object_dictionary['wms'] = {
                    'png': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/png'),
                    'png8': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/png8'),
                    'jpeg': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/jpeg'),
                    'gif': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/gif'),
                    'tiff': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/tiff'),
                    'tiff8': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/tiff8'),
                    'geotiff': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/geotiff'),
                    'geotiff8': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/geotiff8'),
                    'svg': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/svg'),
                    'pdf': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='application/pdf'),
                    'georss': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='rss'),
                    'kml': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='kml'),
                    'kmz': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='kmz'),
                    'openlayers': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='application/openlayers')
                }

            elif object_dictionary['resource_type'] == 'layerGroup':
                # Defaults
                bbox = '-180,-90,180,90'
                srs = 'EPSG:4326'
                width = '512'
                height = '512'
                style = ''

                # Layer and style
                layer = object_dictionary['name']
                if 'default_style' in object_dictionary:
                    style = object_dictionary['default_style']

                # Try to extract the bounding box from the resource which was saved earlier
                if 'bounds' in object_dictionary and object_dictionary['bounds']:
                    # Find the native bounding box
                    nbbox = object_dictionary['bounds']
                    minx = nbbox[0]
                    maxx = nbbox[1]
                    miny = nbbox[2]
                    maxy = nbbox[3]
                    srs = nbbox[4]
                    bbox = '{0},{1},{2},{3}'.format(minx, miny, maxx, maxy)

                    # Resize the width to be proportionate to the image aspect ratio
                    aspect_ratio = (float(maxx) - float(minx)) / (float(maxy) - float(miny))
                    width = str(int(aspect_ratio * float(height)))

                object_dictionary['wms'] = {
                    'png': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/png'),
                    'png8': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/png8'),
                    'jpeg': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/jpeg'),
                    'gif': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/gif'),
                    'tiff': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/tiff'),
                    'tiff8': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/tiff8'),
                    'geptiff': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/geotiff'),
                    'geotiff8': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/geotiff8'),
                    'svg': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='image/svg'),
                    'pdf': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='application/pdf'),
                    'georss': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='rss'),
                    'kml': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='kml'),
                    'kmz': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='kmz'),
                    'openlayers': self._get_wms_url(layer, style, bbox=bbox, srs=srs, width=width, height=height, output_format='application/openlayers')
                }

        return object_dictionary

    def list_resources(self, with_properties=False, store=None, workspace=None, debug=False):
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
        # Get a GeoServer catalog object and query for list of resources
        catalog = self._get_geoserver_catalog_object()
        try:
            resource_objects = catalog.get_resources(store=store, workspace=workspace)
            return self._handle_list(resource_objects, with_properties, debug)
        except geoserver.catalog.AmbiguousRequestError as e:
            response_object = {'success': False,
                               'error': str(e)}
        except TypeError as e:
            response_object = {'success': False,
                               'error': 'Multiple stores found named "{0}".'.format(store)}
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
        # Get a GeoServer catalog object and query for list of layers
        catalog = self._get_geoserver_catalog_object()
        layer_objects = catalog.get_layers()
        return self._handle_list(layer_objects, with_properties, debug)

    def list_layer_groups(self, with_properties=False, debug=False):
        """
        List the names of all layer groups available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of layer group dictionaries instead of a list of layer group names.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_layer_groups()

          response = engine.list_layer_groups(with_properties=True)
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()
        layer_group_objects = catalog.get_layergroups()
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
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()
        workspaces = catalog.get_workspaces()
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
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        try:
            stores = catalog.get_stores(workspace=workspace)
            return self._handle_list(stores, with_properties, debug)

        except AttributeError as e:
            response_dict = {'success': False,
                             'result': 'Invalid workspace "{0}".'.format(workspace)}
        self._handle_debug(response_dict, debug)
        return response_dict

    def list_styles(self, with_properties=False, debug=False):
        """
        List the names of all styles available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of style dictionaries instead of a list of style names.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.list_styles()

          response = engine.list_styles(with_properties=True)
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()
        styles = catalog.get_styles()
        return self._handle_list(styles, with_properties, debug)

    def get_resource(self, resource_id, store=None, debug=False):
        """
        Retrieve a resource object.

        Args:
          resource_id (string): Identifier of the resource to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
          store (string, optional): Get resource from this store.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_resource('example_workspace:resource_name')

          response = engine.get_resource('resource_name', store='example_store')

        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(resource_id)

        # Get resource
        try:
            resource = catalog.get_resource(name=name, store=store, workspace=workspace)
            if not resource:
                response_dict = {'success': False,
                                 'error': 'Resource "{0}" not found.'.format(resource_id)}
            else:
                resource_dict = self._transcribe_geoserver_object(resource)

                # Assemble Response
                response_dict = {'success': True,
                                 'result': resource_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_layer(self, layer_id, debug=False):
        """
        Retrieve a layer object.

        Args:
          layer_id (string): Identifier of the layer to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_layer('layer_name')

          response = engine.get_layer('workspace_name:layer_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        try:
            # Get layer
            layer = catalog.get_layer(name=layer_id)

            if not layer:
                response_dict = {'success': False,
                                 'error': 'Layer "{0}" not found.'.format(layer_id)}
            else:
                layer_dict = self._transcribe_geoserver_object(layer)

                # Get layer caching properties (gsconfig doesn't support this)
                gwc_url = '{0}layers/{1}.xml'.format(self.gwc_endpoint, layer_id)
                auth = (self.username, self.password)
                r = requests.get(gwc_url, auth=auth)

                if r.status_code == 200:
                    root = ElementTree.XML(r.text)
                    tile_caching_dict = ConvertXmlToDict(root)
                    layer_dict['tile_caching'] = tile_caching_dict['GeoServerLayer']

                # Assemble Response
                response_dict = {'success': True,
                                 'result': layer_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_layer_group(self, layer_group_id, debug=False):
        """
        Retrieve a layer group object.

        Args:
          layer_group_id (string): Identifier of the layer group to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_layer_group('layer_group_name')

          response = engine.get_layer_group('workspace_name:layer_group_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        try:
            # Get resource
            layer_group = catalog.get_layergroup(name=layer_group_id)

            if not layer_group:
                response_dict = {'success': False,
                                 'error': 'Layer Group "{0}" not found.'.format(layer_group_id)}
            else:
                layer_group_dict = self._transcribe_geoserver_object(layer_group)

                # Assemble Response
                response_dict = {'success': True,
                                 'result': layer_group_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_store(self, store_id, debug=False):
        """
        Retrieve a store object.

        Args:
          store_id (string): Identifier of the store to retrieve. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.get_store('store_name')

          response = engine.get_store('workspace_name:store_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(store_id)

        try:
            # Get resource
            store = catalog.get_store(name=name, workspace=workspace)

            if not store:
                response_dict = {'success': False,
                                 'error': 'Store "{0}" not found.'.format(store_id)}
            else:
                store_dict = self._transcribe_geoserver_object(store)

                # Assemble Response
                response_dict = {'success': True,
                                 'result': store_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

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
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        try:
            # Get resource
            workspace = catalog.get_workspace(name=workspace_id)

            if not workspace:
                response_dict = {'success': False,
                                 'error': 'Workspace "{0}" not found.'.format(workspace_id)}
            else:
                workspace_dict = self._transcribe_geoserver_object(workspace)

                # Assemble Response
                response_dict = {'success': True,
                                 'result': workspace_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

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
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(style_id)

        try:
            # Get style
            style = catalog.get_style(name=name, workspace=workspace)

            if not style:
                response_dict = {'success': False,
                                 'error': 'Workspace "{0}" not found.'.format(style_id)}
            else:
                style_dict = self._transcribe_geoserver_object(style)

                # Assemble Response
                response_dict = {'success': True,
                                 'result': style_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def link_sqlalchemy_db_to_geoserver(self, store_id, sqlalchemy_engine, docker=False, debug=False,
                                        docker_ip_address='172.17.42.1'):
        """
        Helper function to simplify linking postgis databases to geoservers using the sqlalchemy engine object.

        Args:
          store_id (string): Identifier for the store to add the resource to. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
          sqlalchemy_engine (sqlalchemy_engine): An SQLAlchemy engine object.
          docker (bool, optional): Set to True if the database and geoserver are running in a Docker container. Defaults to False.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
          docker_ip_address (str, optional): Override the docker network ip address. Defaults to '172.17.41.1'.

        Returns:
          (dict): Response dictionary
        """
        connection_dict = sqlalchemy_engine.url.translate_connect_args()
        response = self.create_postgis_feature_resource(
            store_id=store_id,
            host=docker_ip_address if docker else connection_dict['host'],
            port=connection_dict['port'],
            database=connection_dict['database'],
            user=connection_dict['username'],
            password=connection_dict['password'],
            debug=debug
        )
        return response

    def create_postgis_feature_resource(self, store_id, host, port, database, user, password, table=None, debug=False):
        """
        Use this method to link an existing PostGIS database to GeoServer as a feature store. Note that this method only works for data in vector formats.

        Args:
          store_id (string): Identifier for the store to add the resource to. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
          host (string): Host of the PostGIS database (e.g.: 'www.example.com').
          port (string): Port of the PostGIS database (e.g.: '5432')
          database (string): Name of the database.
          user (string): Database user that has access to the database.
          password (string): Password of database user.
          table (string, optional): Name of existing table to add as a feature resource to the newly created feature store. A layer will automatically be created for the feature resource as well. Both the layer and the resource will share the same name as the table.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          # With Table

          response = engine.create_postgis_feature_resource(store_id='workspace:store_name', table='table_name', host='localhost', port='5432', database='database_name', user='user', password='pass')

          # Without table

          response = engine.create_postgis_resource(store_id='workspace:store_name', host='localhost', port='5432', database='database_name', user='user', password='pass')

        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = catalog.get_default_workspace().name

        # Determine if store exists
        try:
            catalog.get_store(name=name, workspace=workspace)
            store_exists = True
        except geoserver.catalog.FailedRequestError:
            store_exists = False

        # Create the store if it doesn't exist already
        if not store_exists:
            xml = """
                  <dataStore>
                    <name>{0}</name>
                    <connectionParameters>
                      <host>{1}</host>
                      <port>{2}</port>
                      <database>{3}</database>
                      <user>{4}</user>
                      <passwd>{5}</passwd>
                      <dbtype>postgis</dbtype>
                    </connectionParameters>
                  </dataStore>
                  """.format(name, host, port, database, user, password)

            # Prepare headers
            headers = {
                "Content-type": "text/xml",
                "Accept": "application/xml"
            }

            # Prepare URL to create store
            url = self._assemble_url('workspaces', workspace, 'datastores')

            # Execute: POST /workspaces/<ws>/datastores
            response = requests.post(url=url,
                                     data=xml,
                                     headers=headers,
                                     auth=HTTPBasicAuth(username=self.username, password=self.password))

            # Return with error if this doesn't work
            if response.status_code != 201:
                response_dict = {'success': False,
                                 'error': '{1}({0}): {2}'.format(response.status_code, response.reason, response.text)}

                self._handle_debug(response_dict, debug)
                return response_dict

        if not table:
            # Wrap up successfully with new store created
            catalog.reload()
            new_store = catalog.get_store(name=name, workspace=workspace)
            resource_dict = self._transcribe_geoserver_object(new_store)

            response_dict = {'success': True,
                             'result': resource_dict}

            self._handle_debug(response_dict, debug)
            return response_dict

        # Throw error if resource already exists
        try:
            resource = catalog.get_resource(name=table, workspace=workspace)
            if resource:
                message = "There is already a resource named " + table

                if workspace:
                    message += " in " + workspace

                response_dict = {'success': False,
                                 'error': message}

                self._handle_debug(response_dict, debug)
                return response_dict

        except geoserver.catalog.FailedRequestError:
            pass

        # Prepare file for adding the table
        xml = """
              <featureType>
                <name>{0}</name>
              </featureType>
              """.format(table)

        # Prepare headers
        headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        # Prepare URL
        url = self._assemble_url('workspaces', workspace, 'datastores', name, 'featuretypes')

        # Execute: POST /workspaces/<ws>/datastores
        response = requests.post(url=url,
                                 data=xml,
                                 headers=headers,
                                 auth=HTTPBasicAuth(username=self.username, password=self.password))

        # Handle failure
        if response.status_code != 201:
            response_dict = {'success': False,
                             'error': '{1}({0}): {2}'.format(response.status_code, response.reason, response.text)}

            self._handle_debug(response_dict, debug)
            return response_dict

        # Wrap up successfully
        catalog.reload()
        new_resource = catalog.get_resource(name=table, store=name)
        resource_dict = self._transcribe_geoserver_object(new_resource)

        response_dict = {'success': True,
                         'result': resource_dict}
        self._handle_debug(response_dict, debug)
        return response_dict

    def add_table_to_postgis_store(self, store_id, table, debug=True):
        """
        Add an existing postgis table as a feature resource to a postgis store that already exists.

        Args
          store_id (string): Identifier for the store to add the resource to. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
          table (string): Name of existing table to add as a feature resource. A layer will automatically be created for this resource. Both the resource and the layer will share the same name as the table.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.add_table_to_postgis_store(store_id='workspace:store_name', table='table_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = catalog.get_default_workspace().name

        # Throw error store does not exist
        try:
            catalog.get_store(name=name, workspace=workspace)
        except geoserver.catalog.FailedRequestError:
            message = "There is no store named " + name
            if workspace:
                message += " in " + workspace

            response_dict = {'success': False,
                             'error': message}

            self._handle_debug(response_dict, debug)
            return response_dict

        # Prepare file
        xml = """
              <featureType>
                <name>{0}</name>
              </featureType>
              """.format(table)

        # Prepare headers
        headers = {
            "Content-type": "text/xml",
            "Accept": "application/xml"
        }

        # Prepare URL
        url = self._assemble_url('workspaces', workspace, 'datastores', name, 'featuretypes')

        # Execute: POST /workspaces/<ws>/datastores
        response = requests.post(url=url,
                                 data=xml,
                                 headers=headers,
                                 auth=HTTPBasicAuth(username=self.username, password=self.password))

        if response.status_code != 201:
            response_dict = {'success': False,
                             'error': '{1}({0}): {2}'.format(response.status_code, response.reason, response.text)}

            self._handle_debug(response_dict, debug)
            return response_dict

        # Wrap up successfully
        catalog.reload()
        new_store = catalog.get_store(name=name, workspace=workspace)
        resource_dict = self._transcribe_geoserver_object(new_store)

        response_dict = {'success': True,
                         'result': resource_dict}
        self._handle_debug(response_dict, debug)
        return response_dict

    def create_sql_view(self, feature_type_name, postgis_store_id, sql, geometry_column, geometry_type,
                        geometry_srid=4326, default_style_id=None, key_column=None, parameters=None, debug=False):
        """
        Create a new feature type configured as an SQL view.

        Args
          feature_type_name (string): Name of the feature type and layer to be created.
          postgis_store_id (string): Identifier of existing postgis store with tables that will be queried by the sql view. Can be a store name or a workspace-name combination (e.g.: "name" or "workspace:name").
          sql (string): SQL that will be used to construct the sql view / virtual table.
          geometry_column (string): Name of the geometry column.
          geometry_type (string): Type of the geometry column (e.g. "Point", "LineString", "Polygon").
          geometry_srid (string, optional): EPSG spatial reference id of the geometry column. Defaults to 4326.
          default_style (string, optional): Identifier of a style to assign as the default style. Can be a style name or a workspace-name combination (e.g.: "name" or "workspace:name").
          key_column (string, optional): The name of the key column.
          parameters (iterable, optional): A list/tuple of tuple-triplets representing parameters in the form (name, default, regex_validation), (e.g.: (('variable', 'pressure', '^[\w]+$'), ('simtime', '0:00:00', '^[\w\:]+$'))
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

            sql = "SELECT name, value, geometry FROM pipes"

            response = engine.create_sql_view(
                feature_type_name='my_feature_type',
                postgis_store_id='my_workspace:my_postgis_store',
                sql=sql,
                geometry_column='geometry',
                geometry_type='LineString',
                geometry_srid=32144,
                default_style_id='my_workspace:pipes',
                debug=True
            )

        """
        # Get a catalog object
        catalog = self._get_geoserver_catalog_object()

        # Get Existing PostGIS Store
        store_name = postgis_store_id
        store_workspace_name = None
        if ':' in postgis_store_id:
            store_workspace_name, store_name = postgis_store_id.split(':')
        store = catalog.get_store(store_name, workspace=store_workspace_name)

        # Define virtual table / sql view
        epsg_code = 'EPSG:{0}'.format(geometry_srid)
        geometry = JDBCVirtualTableGeometry(geometry_column, geometry_type, str(geometry_srid))

        if parameters is not None:
            jdbc_parameters = []
            for parameter_args in parameters:
                jdbc_parameters.append(JDBCVirtualTableParam(*parameter_args))
            parameters = jdbc_parameters

        sql_view = JDBCVirtualTable(feature_type_name, sql, 'false', geometry, key_column, parameters)

        # Publish Feature Type
        catalog.publish_featuretype(feature_type_name, store, epsg_code, jdbc_virtual_table=sql_view)

        # Wrap Up
        catalog.reload()
        r_feature_layer = catalog.get_layer(feature_type_name)

        if default_style_id is None:
            resource_dict = self._transcribe_geoserver_object(r_feature_layer)
            response_dict = {'success': True,
                             'result': resource_dict}
            self._handle_debug(response_dict, debug)
            return response_dict

        # Associate Style
        style_name = default_style_id
        style_workspace = None

        if ':' in default_style_id:
            style_workspace, style_name = default_style_id.split(':')
        style = catalog.get_style(style_name, workspace=style_workspace)
        r_feature_layer.default_style = style
        catalog.save(r_feature_layer)
        catalog.reload()
        resource_dict = self._transcribe_geoserver_object(r_feature_layer)
        response_dict = {'success': True,
                         'result': resource_dict}
        self._handle_debug(response_dict, debug)
        return response_dict

    def create_shapefile_resource(self, store_id, shapefile_base=None, shapefile_zip=None, shapefile_upload=None, overwrite=False, charset=None, debug=False):
        """
         Use this method to add shapefile resources to GeoServer.

         This method will result in the creation of three items: a feature type store, a feature type resource, and a layer. If store_id references a store that does not exist, it will be created. The feature type resource and the subsequent layer will be created with the same name as the feature type store. Provide shapefile with either shapefile_base, shapefile_zip, or shapefile_upload arguments.

        Args
          store_id (string): Identifier for the store to add the resource to. Can be a store name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
          shapefile_base (string, optional): Path to shapefile base name (e.g.: "/path/base" for shapefile at "/path/base.shp")
          shapefile_zip (string, optional): Path to a zip file containing the shapefile and side cars.
          shapefile_upload (FileUpload list, optional): A list of Django FileUpload objects containing a shapefile and side cars that have been uploaded via multipart/form-data form.
          overwrite (bool, optional): Overwrite the file if it already exists.
          charset (string, optional): Specify the character encoding of the file being uploaded (e.g.: ISO-8559-1)
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
        arg_value_error_msg = 'Exactly one of the "shapefile_base", "shapefile_zip", ' \
                              'or "shapefile_upload" arguments must be specified. '

        if not shapefile_base and not shapefile_zip and not shapefile_upload:
            raise ValueError(arg_value_error_msg + 'None given.')

        elif shapefile_zip and shapefile_upload and shapefile_base:
            raise ValueError(arg_value_error_msg + '"shapefile_base", "shapefile_zip", and '
                                                   '"shapefile_upload" given.')

        elif shapefile_base and shapefile_zip:
            raise ValueError(arg_value_error_msg + '"shapefile_base" and "shapefile_zip" given.')

        elif shapefile_base and shapefile_upload:
            raise ValueError(arg_value_error_msg + '"shapefile_base" and "shapefile_upload" given.')

        elif shapefile_zip and shapefile_upload:
            raise ValueError(arg_value_error_msg + '"shapefile_zip" and "shapefile_upload" given.')

        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = catalog.get_default_workspace().name

        # Throw error if overwrite is not true and store already exists
        if not overwrite:
            try:
                store = catalog.get_store(name=name, workspace=workspace)
                message = "There is already a store named " + name
                if workspace:
                    message += " in " + workspace

                response_dict = {'success': False,
                                 'error': message}

                self._handle_debug(response_dict, debug)
                return response_dict

            except geoserver.catalog.FailedRequestError:
                pass
            except:
                raise

        # Prepare files
        temp_archive = None
        zip_file_in_memory = None

        # Shapefile Base Case
        if shapefile_base:
            shapefile_plus_sidecars = shapefile_and_friends(shapefile_base)
            temp_archive = '{0}.zip'.format(os.path.join(os.path.split(shapefile_base)[0], name))

            with ZipFile(temp_archive, 'w') as zfile:
                for extension, filepath in shapefile_plus_sidecars.iteritems():
                    filename = '{0}.{1}'.format(name, extension)
                    zfile.write(filename=filepath, arcname=filename)

            files = {'file': open(temp_archive, 'rb')}

        # Shapefile Zip Case
        elif shapefile_zip:
            if is_zipfile(shapefile_zip):
                files = {'file': open(shapefile_zip, 'rb')}
            else:
                raise TypeError('"{0}" is not a zip archive.'.format(shapefile_zip))

        # Shapefile Upload Case
        elif shapefile_upload:
            # Write files in memory to zipfile in memory
            zip_file_in_memory = BytesIO()

            with ZipFile(zip_file_in_memory, 'w') as zfile:
                for file in shapefile_upload:
                    extension = os.path.splitext(file.name)[1]
                    filename = '{0}{1}'.format(name, extension)
                    zfile.writestr(filename, file.read())

            files = {'file': zip_file_in_memory.getvalue()}

        else:
            raise TypeError('Shapefile error. Check that you are using the correct shapefile argument and that the '
                            'files are formatted correctly.')

        # Prepare headers
        headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml"
        }

        # Prepare URL
        url = self._assemble_url('workspaces', workspace, 'datastores', name, 'file.shp')

        # Set params
        params = {}

        if charset:
            params['charset'] = charset

        if overwrite:
            params['update'] = 'overwrite'

        # Execute: PUT /workspaces/<ws>/datastores/<ds>/file.shp
        response = requests.put(url=url,
                                files=files,
                                headers=headers,
                                params=params,
                                auth=HTTPBasicAuth(username=self.username, password=self.password))

        # Clean up file stuff
        if temp_archive:
            os.remove(temp_archive)

        if zip_file_in_memory:
            zip_file_in_memory.close()

        # Wrap up with failure
        if response.status_code != 201:
            response_dict = {'success': False,
                             'error': '{1}({0}): {2}'.format(response.status_code, response.reason, response.text)}

            self._handle_debug(response_dict, debug)
            return response_dict

        # Wrap up successfully
        catalog.reload()
        new_resource = catalog.get_resource(name=name, workspace=workspace)
        resource_dict = self._transcribe_geoserver_object(new_resource)

        response_dict = {'success': True,
                         'result': resource_dict}
        self._handle_debug(response_dict, debug)
        return response_dict

    def create_coverage_resource(self, store_id, coverage_type, coverage_file=None,
                                 coverage_upload=None, coverage_name=None,
                                 overwrite=False, debug=False):
        """
        Use this method to add coverage resources to GeoServer.

        This method will result in the creation of three items: a coverage store, a coverage resource, and a layer. If store_id references a store that does not exist, it will be created. Unless coverage_name is specified, the coverage resource and the subsequent layer will be created with the same name as the image file that is uploaded.

        Args
          store_id (string): Identifier for the store to add the image to or to be created. Can be a name or a workspace name combination (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace. If no workspace is given, the default workspace will be assigned.
          coverage_type (string): Type of coverage that is being created. Valid values include: 'geotiff', 'worldimage', 'imagemosaic', 'imagepyramid', 'gtopo30', 'arcgrid', 'grassgrid', 'erdasimg', 'aig', 'gif', 'png', 'jpeg', 'tiff', 'dted', 'rpftoc', 'rst', 'nitf', 'envihdr', 'mrsid', 'ehdr', 'ecw', 'netcdf', 'erdasimg', 'jp2mrsid'.
          coverage_file (string, optional): Path to the coverage image or zip archive. Most files will require a .prj file with the Well Known Text definition of the projection. Zip this file up with the image and send the archive.
          coverage_upload (FileUpload list, optional): A list of Django FileUpload objects containing a coverage file and .prj file or archive that have been uploaded via multipart/form-data form.
          coverage_name (string): Name of the coverage resource and subsequent layer that are created. If unspecified, these will match the name of the image file that is uploaded.
          overwrite (bool, optional): Overwrite the file if it already exists.
          charset (string, optional): Specify the character encoding of the file being uploaded (e.g.: ISO-8559-1)
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Note
          If the type coverage being uploaded includes multiple files (e.g.: image, world file, projecttion file), they must be uploaded as a zip archive. Otherwise upload the single file.

        Returns:
          (dict): Response dictionary

        Examples:

          coverage_file = '/path/to/geotiff/example.zip'

          response = engine.create_coverage_resource(store_id='workspace:store_name', coverage_file=coverage_file, coverage_type='geotiff')
        """
        # Globals
        VALID_COVERAGE_TYPES = ('geotiff',
                                'worldimage',
                                'imagemosaic',
                                'imagepyramid',
                                'gtopo30',
                                'arcgrid',
                                'grassgrid',
                                'erdasimg',
                                'aig',
                                'gif',
                                'png',
                                'jpeg',
                                'tiff',
                                'dted',
                                'rpftoc',
                                'rst',
                                'nitf',
                                'envihdr',
                                'mrsid',
                                'ehdr',
                                'ecw',
                                'netcdf',
                                'erdasimg',
                                'jp2mrsid')

        # Validate coverage type
        if coverage_type not in VALID_COVERAGE_TYPES:
            raise ValueError('"{0}" is not a valid coverage_type. Use either {1}'.format(coverage_type, ', '.join(VALID_COVERAGE_TYPES)))

        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get default work space if none is given
        if not workspace:
            workspace = catalog.get_default_workspace().name

        # Throw error if overwrite is not true and store already exists
        if not overwrite:
            try:
                store = catalog.get_store(name=name, workspace=workspace)
                message = "There is already a store named " + name
                if workspace:
                    message += " in " + workspace

                response_dict = {'success': False,
                                 'error': message}

                self._handle_debug(response_dict, debug)
                return response_dict

            except geoserver.catalog.FailedRequestError:
                pass

        # Prepare files
        working_dir = None

        if coverage_type == 'grassgrid' and coverage_file is not None:
            working_dir = os.path.join(os.path.dirname(coverage_file), '.gstmp')

            # Unzip
            zip_file = ZipFile(coverage_file)
            zip_file.extractall(working_dir)

            # Change Header
            valid_grass_file = False

            for file in os.listdir(working_dir):
                if 'prj' not in file:
                    # Defaults
                    contents = ''
                    north = 90.0
                    south = -90.0
                    east = -180.0
                    west = 180.0
                    rows = 360
                    cols = 720

                    with open(os.path.join(working_dir, file), 'r') as f:
                        contents = f.readlines()

                    corrupt_file = False

                    for line in contents[0:6]:
                        if 'north' in line:
                            north = float(line.split(':')[1].strip())
                        elif 'south' in line:
                            south = float(line.split(':')[1].strip())
                        elif 'east' in line:
                            east = float(line.split(':')[1].strip())
                        elif 'west' in line:
                            west = float(line.split(':')[1].strip())
                        elif 'rows' in line:
                            rows = int(line.split(':')[1].strip())
                        elif 'cols' in line:
                            cols = int(line.split(':')[1].strip())
                        else:
                            corrupt_file = True

                    if corrupt_file:
                        break

                    # Calcuate new header
                    xllcorner = east
                    yllcorner = south
                    cellsize = (north - south) / rows

                    header = ['ncols         {0}\n'.format(cols),
                              'nrows         {0}\n'.format(rows),
                              'xllcorner     {0}\n'.format(xllcorner),
                              'yllcorner     {0}\n'.format(yllcorner),
                              'cellsize      {0}\n'.format(cellsize)]

                    # Strip off old header and add new one
                    for i in range(0,6):
                        contents.pop(0)
                    contents = header + contents

                    with open(os.path.join(working_dir, file), 'w') as f:
                        for line in contents:
                            f.write(line)

                    valid_grass_file = True

            if not valid_grass_file:
                # Clean up
                for file in os.listdir(working_dir):
                    os.remove(os.path.join(working_dir, file))
                os.rmdir(working_dir)
                raise IOError('GRASS file could not be processed, check to ensure the GRASS grid is correctly formatted or included.')

            # New coverage zip file (rezip)
            coverage_file = os.path.join(working_dir, 'foo.zip')
            with ZipFile(coverage_file, 'w') as zf:
                for file in os.listdir(working_dir):
                    if file != 'foo.zip':
                        zf.write(os.path.join(working_dir, file), file)

        # Prepare file(s) for upload
        files = None
        data = None

        if coverage_file is not None:
            if is_zipfile(coverage_file):
                files = {'file': open(coverage_file, 'rb')}
                content_type = 'application/zip'
            else:
                content_type = 'image/{0}'.format(coverage_type)
                data = open(coverage_file, 'rb')

        elif coverage_upload is not None:
            content_type = 'application/zip'

            # Check if zip archive
            try:
                if coverage_upload.name.endswith('.zip'):
                    files = {'file': coverage_upload }
                else:
                    content_type = 'image/{0}'.format(coverage_type)
                    data = coverage_upload

            except AttributeError:
                pass

            if files is None and data is None:
                # Write files in memory to zipfile in memory
                zip_file_in_memory = BytesIO()

                with ZipFile(zip_file_in_memory, 'w') as zfile:
                    for file in coverage_upload:
                        zfile.writestr(file.name, file.read())
                files = {'file': zip_file_in_memory.getvalue()}


        # Prepare headers
        extension = coverage_type

        if coverage_type == 'grassgrid':
            extension = 'arcgrid'

        headers = {
            "Content-type": content_type,
            "Accept": "application/xml"
        }

        # Prepare URL
        url = self._assemble_url('workspaces', workspace, 'coveragestores', name, 'file.{0}'.format(extension))

        # Set params
        params = {}

        if coverage_name:
            params['coverageName'] = coverage_name

        if overwrite:
            params['update'] = 'overwrite'

        # Execute: PUT /workspaces/<ws>/datastores/<ds>/file.shp
        response = requests.put(url=url,
                                files=files,
                                data=data,
                                headers=headers,
                                params=params,
                                auth=HTTPBasicAuth(username=self.username, password=self.password))

        # Clean up
        if working_dir:
            for file in os.listdir(working_dir):
                os.remove(os.path.join(working_dir, file))
            os.rmdir(working_dir)

        if response.status_code != 201:
            response_dict = {'success': False,
                             'error': '{1}({0}): {2}'.format(response.status_code, response.reason, response.text)}

            self._handle_debug(response_dict, debug)
            return response_dict

        # Wrap up successfully
        catalog.reload()
        new_resource = catalog.get_resource(name=name, workspace=workspace)
        resource_dict = self._transcribe_geoserver_object(new_resource)

        response_dict = {'success': True,
                         'result': resource_dict}
        self._handle_debug(response_dict, debug)
        return response_dict

    def create_layer_group(self, layer_group_id, layers, styles, bounds=None, debug=False):
        """
        Create a layer group. The number of layers and the number of styles must be the same.

        Args:
          layer_group_id (string): Identifier of the layer group to create.
          layers (iterable): A list of layer names to be added to the group. Must be the same length as the styles list.
          styles (iterable): A list of style names to  associate with each layer in the group. Must be the same length as the layers list.
          bounds (iterable): A tuple representing the bounding box of the layer group (e.g.: ('-74.02722', '-73.907005', '40.684221', '40.878178', 'EPSG:4326') )
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          layers = ('layer1', 'layer2')

          styles = ('style1', 'style2')

          bounds = ('-74.02722', '-73.907005', '40.684221', '40.878178', 'EPSG:4326')

          response = engine.create_layer_group(layer_group_id='layer_group_name', layers=layers, styles=styles, bounds=bounds)
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Response dictionary
        response_dict = {'success': False}

        # Create layer group
        try:
            layer_group = catalog.create_layergroup(layer_group_id, layers, styles, bounds)
            catalog.save(layer_group)

            layer_group_dict = self._transcribe_geoserver_object(layer_group)

            response_dict['success'] = True
            response_dict['result'] = layer_group_dict

        except geoserver.catalog.ConflictingDataError as e:
            response_dict['success'] = False
            response_dict['error'] = str(e)

        except geoserver.catalog.FailedRequestError as e:
            response_dict['success'] = False
            response_dict['error'] = str(e)

        self._handle_debug(response_dict, debug)
        return response_dict

    def create_workspace(self, workspace_id, uri, debug=False):
        """
        Create a new workspace.

        Args:
          workspace_id (string): Identifier of the workspace to create. Must be unique.
          uri (string): URI associated with your project. Does not need to be a real web URL, just a unique identifier. One suggestion is to append the URL of your project with the name of the workspace (e.g.: http:www.example.com/workspace-name).
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.create_workspace(workspace_id='workspace_name', uri='www.example.com/workspace_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Create workspace
        try:
            # Do create
            workspace = catalog.create_workspace(workspace_id, uri)
            workspace_dict = self._transcribe_geoserver_object(workspace)
            response_dict = {'success': True,
                             'result': workspace_dict}

        except AssertionError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def create_style(self, style_id, sld, overwrite=False, debug=False):
        """
        Create a new SLD style object.

        Args:
          style_id (string): Identifier of the style to create.
          sld (string): Styled Layer Descriptor string
          overwrite (bool, optional): Overwrite if style already exists. Defaults to False.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          sld = '/path/to/style.sld'

          sld_file = open(sld, 'r')

          response = engine.create_style(style_id='fred', sld=sld_file.read(), debug=True)

          sld_file.close()
        """
        # Get a GeoServer catalog object
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(style_id)

        # Create workspace
        try:
            # Do create
            num_attempts = 0
            upload_error = True
            
            while num_attempts < 5 and upload_error:

                try:
                    catalog.create_style(name=name,
                                         data=sld,
                                         workspace=workspace,
                                         overwrite=overwrite)
                    upload_error = False
                except geoserver.catalog.UploadError as e:
                    num_attempts += 1
                    upload_error = e

            if upload_error:
                raise upload_error

            catalog.reload()
            style = catalog.get_style(name=name, workspace=workspace)

            style_dict = self._transcribe_geoserver_object(style)
            response_dict = {'success': True,
                             'result': style_dict}

        except AssertionError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        except geoserver.catalog.ConflictingDataError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_resource(self, resource_id, store=None, debug=False, **kwargs):
        """
        Update an existing resource.

        Args:
          resource_id (string): Identifier of the resource to update. Can be a name or a workspace-name combination (e.g.: "name" or "workspace:name").
          store (string, optional): Update a resource in this store.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Key value pairs representing the attributes and values to change.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.update_resource(resource_id='workspace:resource_name', enabled=False, title='New Title')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(resource_id)

        try:
            # Get resource
            resource = catalog.get_resource(name=name, store=store, workspace=workspace)

            # Make the changes
            updated_resource = self._apply_changes_to_gs_object(kwargs, resource)

            # Save the changes
            catalog.save(updated_resource)

            # Return the updated resource dictionary
            resource_dict = self._transcribe_geoserver_object(updated_resource)

            # Assemble Response
            response_dict = {'success': True,
                             'result': resource_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_layer(self, layer_id, debug=False, **kwargs):
        """
        Update an existing layer.

        Args:
          layer_id (string): Identifier of the layer to update.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Key value pairs representing the attributes and values to change.

        Returns:
          (dict): Response dictionary

        Examples:

          updated_layer = engine.update_layer(layer_id='workspace:layer_name', default_style='style1', styles=['style1', 'style2'])
        """
        # Pop tile caching properties to handle separately
        tile_caching = kwargs.pop('tile_caching', None)

        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        try:
            # Get resource
            layer = catalog.get_layer(name=layer_id)

            # Apply changes from kwargs
            updated_layer = self._apply_changes_to_gs_object(kwargs, layer)

            # Save the changes
            catalog.save(updated_layer)

            # Return the updated resource dictionary
            layer_dict = self._transcribe_geoserver_object(updated_layer)

            # Assemble Response
            response_dict = {'success': True,
                             'result': layer_dict}

            # Handle tile caching properties (gsconfig doesn't support this)
            if tile_caching is not None:
                gwc_url = '{0}layers/{1}.xml'.format(self.gwc_endpoint, layer_id)
                auth = (self.username, self.password)
                xml = ConvertDictToXml({'GeoServerLayer': tile_caching})
                r = requests.post(
                    gwc_url,
                    auth=auth,
                    headers={'Content-Type': 'text/xml'},
                    data=ElementTree.tostring(xml)
                )

                if r.status_code == 200:
                    layer_dict['tile_caching'] = tile_caching
                    response_dict = {'success': True,
                                     'result': layer_dict}
                else:
                    response_dict = {'success': False,
                                     'error': r.text}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

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

          updated_layer_group = engine.update_layer_group(layer_group_id='layer_group_name', layers=['layer1', 'layer2'], styles=['style1', 'style2'])
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        try:
            # Get resource
            layer_group = catalog.get_layergroup(name=layer_group_id)

            # Make the changes
            for attribute, value in kwargs.iteritems():
                if hasattr(layer_group, attribute):
                    setattr(layer_group, attribute, value)

            # Save the changes
            catalog.save(layer_group)

            # Return the updated resource dictionary
            layer_group_dict = self._transcribe_geoserver_object(layer_group)

            # Assemble Response
            response_dict = {'success': True,
                             'result': layer_group_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': str(e)}

        self._handle_debug(response_dict, debug)
        return response_dict

    def delete_resource(self, resource_id, store=None, purge=False, recurse=False, debug=False):
        """
        Delete a resource.

        Args:
          resource_id (string): Identifier of the resource to delete.
          store (string, optional): Delete resource from this store.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively any dependencies if True (i.e.: layers or layer groups it belongs to).
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.delete_resource('workspace:resource_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(resource_id)

        # Get resource
        resource = catalog.get_resource(name=name, store=store, workspace=workspace)

        # Handle delete
        return self._handle_delete(identifier=resource_id, gs_object=resource, purge=purge,
                                   recurse=recurse, debug=debug)

    def delete_layer(self, layer_id, purge=False, recurse=False, debug=False):
        """
        Delete a layer.

        Args:
          layer_id (string): Identifier of the layer to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True (i.e: delete layer groups it belongs to).
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.delete_layer('workspace:layer_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get resource
        layer = catalog.get_layer(name=layer_id)

        # Handle delete
        return self._handle_delete(identifier=layer_id, gs_object=layer, purge=purge,
                                   recurse=recurse, debug=debug)

    def delete_layer_group(self, layer_group_id, purge=False, recurse=False, debug=False):
        """
        Delete a layer group.

        Args:
          layer_group_id (string): Identifier of the layer group to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.delete_layer_group('layer_group_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get layer group
        layer_group = catalog.get_layergroup(name=layer_group_id)

        # Handle delete
        return self._handle_delete(identifier=layer_group_id, gs_object=layer_group, purge=purge,
                                   recurse=recurse, debug=debug)

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
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get layer group
        workspace = catalog.get_workspace(workspace_id)

        # Handle delete
        return self._handle_delete(identifier=workspace_id, gs_object=workspace, purge=purge,
                                   recurse=recurse, debug=debug)

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
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(store_id)

        # Get layer group
        try:
            store = catalog.get_store(name=name, workspace=workspace)

            # Handle delete
            return self._handle_delete(identifier=store_id, gs_object=store, purge=purge,
                                       recurse=recurse, debug=debug)
        except geoserver.catalog.FailedRequestError as e:
            # Update response dictionary
            response_dict = {'success': False,
                             'error': str(e)}

            self._handle_debug(response_dict, debug)
            return response_dict

    def delete_style(self, style_id, purge=False, recurse=False, debug=False):
        """
        Delete a style.

        Args:
          style_id (string): Identifier of the style to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the response dictionary to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary

        Examples:

          response = engine.delete_resource('style_name')
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(style_id)

        # Get layer group
        try:
            style = catalog.get_style(name=name, workspace=workspace)

            # Handle delete
            return self._handle_delete(identifier=style_id, gs_object=style, purge=purge,
                                       recurse=recurse, debug=debug)
        except geoserver.catalog.FailedRequestError as e:
            # Update response dictionary
            response_dict = {'success': False,
                             'error': str(e)}

            self._handle_debug(response_dict, debug)
            return response_dict

    def validate(self):
        """
        Validate the GeoServer spatial dataset engine. Will throw and error if not valid.
        """
        try:
            r = requests.get(self.endpoint, auth=(self.username, self.password))

        except requests.exceptions.MissingSchema:
            raise AssertionError('The URL "{0}" provided for the GeoServer spatial dataset service endpoint is invalid.'.format(self.endpoint))

        if r.status_code == 401:
            raise AssertionError('The username and password of the GeoServer spatial dataset service engine are not valid.')

        if r.status_code != 200:
            raise AssertionError('The URL "{0}" is not a valid GeoServer spatial dataset service endpoint.'.format(self.endpoint))

        if 'Geoserver Configuration API' not in r.text:
            raise AssertionError('The URL "{0}" is not a valid GeoServer spatial dataset service endpoint.'.format(self.endpoint))


