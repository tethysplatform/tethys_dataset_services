import os
import json
import pprint
import requests
from requests.auth import HTTPBasicAuth
import owslib
import inspect
from zipfile import ZipFile, is_zipfile
import geoserver
from geoserver.catalog import Catalog as GeoServerCatalog, _name
from geoserver.util import shapefile_and_friends

from tethys_dataset_services.base import SpatialDatasetEngine


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

    def _get_geoserver_catalog_object(self):
        """
        Internal method used to get the connection object to GeoServer.
        """
        return GeoServerCatalog(self.endpoint, username=self.username, password=self.password)

    def _get_wfs_url(self, resource_id, output_format='GML3'):
        """
        Assemble a WFS url.
        """
        endpoint = self.endpoint

        # Eliminate trailing slash if necessary
        if endpoint[-1] == '/':
            endpoint = endpoint[:-1]

        if endpoint[-5:] == '/rest':
            endpoint = endpoint[:-5]

        if output_format == 'GML3':
            wfs_url = '{0}/wfs?service=wfs&version=2.0.0&request=GetFeature&typeNames={1}'.format(endpoint, resource_id)
        elif output_format == 'GML2':
            wfs_url = '{0}/wfs?service=wfs&version=1.0.0&request=GetFeature&typeNames={1}&' \
                      'outputFormat=GML2'.format(endpoint, resource_id)
        else:
            wfs_url = '{0}/wfs?service=wfs&version=2.0.0&request=GetFeature&typeNames={1}&' \
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
                response_dict['error'] = e.message

        else:
            # Update response dictionary
            response_dict['success'] = False
            response_dict['error'] = 'GeoServer object does not exist: "{0}".'.format(identifier)

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

        # Get the non-private attributes
        attributes = [a for a in dir(gs_object) if not a.startswith('__') and not a.startswith('_')]

        # Load into a dictionary
        object_dictionary = {}

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
                    if not isinstance(sub_object, str):
                        if sub_object.workspace:
                            object_dictionary[attribute] = '{0}:{1}'.format(sub_object.workspace.name, sub_object.name)
                        else:
                            object_dictionary[attribute] = sub_object.name
                    else:
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
                    'csv': self._get_wfs_url(resource_id, 'csv')
                }
            elif object_dictionary['resource_type'] == 'coverage':
                print('COVERAGE: ', object_dictionary['name'])

        return object_dictionary

    def list_resources(self, with_properties=False, debug=False):
        """
        List all resources available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of resource dictionaries instead of a list of resource names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of resources
        catalog = self._get_geoserver_catalog_object()
        resource_objects = catalog.get_resources()
        return self._handle_list(resource_objects, with_properties, debug)

    def list_layers(self, with_properties=False, debug=False):
        """
        List all layers available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of layer dictionaries instead of a list of layer names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layers
        catalog = self._get_geoserver_catalog_object()
        layer_objects = catalog.get_layers()
        return self._handle_list(layer_objects, with_properties, debug)

    def list_layer_groups(self, with_properties=False, debug=False):
        """
        List all layer groups available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of layer group dictionaries instead of a list of layer group names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()
        layer_group_objects = catalog.get_layergroups()
        return self._handle_list(layer_group_objects, with_properties, debug)

    def list_workspaces(self, with_properties=False, debug=False):
        """
        List all workspaces.

        Args:
          with_properties (bool, optional): Return list of layer group dictionaries instead of a list of layer group names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()
        workspaces = catalog.get_workspaces()
        return self._handle_list(workspaces, with_properties, debug)

    def list_stores(self, workspace=None, with_properties=False, debug=False):
        """
        List all stores.

        Args:
          with_properties (bool, optional): Return list of layer group dictionaries instead of a list of layer group names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
        List all styles.

        Args:
          with_properties (bool, optional): Return list of layer group dictionaries instead of a list of layer group names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()
        styles = catalog.get_styles()
        return self._handle_list(styles, with_properties, debug)

    def get_resource(self, resource_id, store=None, debug=False):
        """
        Retrieve a resource object.

        Args:
          resource_id (string): Identifier for the resource to be created. Can be a name or a workspace name combination to add the new resource to the workspace (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace.
          store (string, optional): Name of the store  from which to get the resource.
          workspace (string, optional): Name of workspace from which to get the resource.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_layer(self, layer_id, debug=False):
        """
        Retrieve a layer object.

        Args:
          layer_id (string): Name of the layer to retrieve.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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

                # Assemble Response
                response_dict = {'success': True,
                                 'result': layer_dict}

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': e.message}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_layer_group(self, layer_group_id, debug=False):
        """
        Retrieve a layer group object.

        Args:
          layer_group_id (string): Name of the layer group to retrieve.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_store(self, store_id, debug=False):
        """
        Retrieve a store object.

        Args:
          store_id (string): Name of the layer group to retrieve.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_workspace(self, workspace_id, debug=False):
        """
        Retrieve a workspace object.

        Args:
          workspace_id (string): Name of the layer group to retrieve.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def get_style(self, style_id, debug=False):
        """
        Retrieve a workspace object.

        Args:
          style_id (string): Name of the layer group to retrieve.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(style_id)

        try:
            # Get style
            style = catalog.get_style(name=name, workspace=workspace)
            print style.name

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
                             'error': e.message}

        # Handle the debug and return
        self._handle_debug(response_dict, debug)
        return response_dict

    def create_resource(self, layer_id, url=None, file=None, **kwargs):
        pass

    def create_shapefile_resource(self, resource_id, shapefile_base, overwrite=False, charset=None, debug=False):
        """
        Create a new shapefile resource.

        Note: Creating a resource results in the creation of a store and a layer.

        Args
          resource_id (string): Identifier for the resource to be created. Can be a name or a workspace name combination to add the new resource to the workspace (e.g.: "name" or "workspace:name"). Note that the workspace must be an existing workspace.
          shapefile_base (string): Path to shapefile base name (e.g.: "/path/base" for shapefile at "/path/base.shp") or a zip file containing all the shapefile components.
          overwrite (bool, optional): Overwrite the file if it already exists.
          charset (string, optional): Specify the character encoding of the file being uploaded (e.g.: ISO-8559-1)

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(resource_id)

        # Get default work space if none is given
        if not workspace:
            workspace = catalog.get_default_workspace().name

        # Throw error if overwrite is not true and store already exists
        if not overwrite:
            try:
                store = catalog.get_store(name=name, workspace=workspace)
                print store.name, store.workspace
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
        if is_zipfile(shapefile_base):
            archive = shapefile_base
        else:
            shapefile_plus_sidecars = shapefile_and_friends(shapefile_base)
            archive = '{0}.zip'.format(os.path.join(os.path.split(shapefile_base)[0], name))

            with ZipFile(archive, 'w') as zfile:
                for extension, filepath in shapefile_plus_sidecars.iteritems():
                    filename = '{0}.{1}'.format(name, extension)
                    zfile.write(filename=filepath, arcname=filename)

        files = {'file': open(archive, 'rb')}

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

        # Clean up file
        os.remove(archive)

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

    def create_coverage_resource(self):
        """
        Create a coverage feature store for raster datasets.
        """
        pass

    def create_layer(self, layer_id, debug=False):
        """
        Create a new layer.

        Args:
          name (string): Name of the layer to create.

        Returns:
          (dict): Response dictionary
        """
        pass

    def create_layer_group(self, layer_group_id, layers, styles, bounds=None, debug=False):
        """
        Create a new resource.

        Args:
          dataset_id (string): Identifier of the dataset to which the resource will be added.
          url (string, optional): URL of resource to associate with resource.
          file (string, optional): Path of file to upload as resource.

        Returns:
          (dict): Response dictionary
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
            response_dict['error'] = e.message

        except geoserver.catalog.FailedRequestError as e:
            response_dict['success'] = False
            response_dict['error'] = e.message

        self._handle_debug(response_dict, debug)
        return response_dict

    def create_workspace(self, workspace_id, uri, debug=False):
        """
        Create a new workspace.

        Args:
          workspace_id (string): Identifier of the workspace.
          uri (string): URI associated with your project. Does not need to be a real web URL, just a unique identifier. One suggestion is to append the URL of your project with the name of the workspace (e.g.: http:www.example.com/workspace-name).
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

        self._handle_debug(response_dict, debug)
        return response_dict

    def create_style(self, style_id, sld, overwrite=False, debug=False):
        """
        Create a new workspace.

        Args:
          create_style (string): Identifier of the style.
          sld (string): Styled Layer Descriptor String
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(style_id)

        # Create workspace
        try:
            # Do create
            catalog.create_style(name=name, data=sld, workspace=workspace, overwrite=overwrite)

            catalog.reload()
            style = catalog.get_style(name=name, workspace=workspace)

            style_dict = self._transcribe_geoserver_object(style)
            response_dict = {'success': True,
                             'result': style_dict}

        except AssertionError as e:
            response_dict = {'success': False,
                             'error': e.message}

        except geoserver.catalog.ConflictingDataError as e:
            response_dict = {'success': False,
                             'error': e.message}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_resource(self, resource_id, store=None, debug=False, **kwargs):
        """
        Update an existing resource.

        Args:
          resource_id (string): Identifier of the resource to update.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Key value pairs representing the attributes to change.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_layer(self, layer_id, debug=False, **kwargs):
        """
        Update an existing layer.

        Args:
          layer_id (string): Identifier of the dataset to update.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
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

        except geoserver.catalog.FailedRequestError as e:
            response_dict = {'success': False,
                             'error': e.message}

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_layer_group(self, layer_group_id, debug=False, **kwargs):
        """
        Update an existing layer. If modifying the layers, ensure the number of layers
        and the number of styles are the same.

        Args:
          layer_group_id (string): Identifier of the dataset to update.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

        self._handle_debug(response_dict, debug)
        return response_dict

    def delete_resource(self, resource_id, store=None, purge=False, recurse=False, debug=False):
        """
        Delete a resource.

        Args:
          resource_id (string): Name of the resource to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
          layer_id (string): Name of the layer to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
          layer_group_id (string): Name of the layer group to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
        Delete a layer group.

        Args:
          workspace_id (string): Name of the workspace to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
          store_id (string): Name of the store to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

            self._handle_debug(response_dict, debug)
            return response_dict

    def delete_style(self, style_id, purge=False, recurse=False, debug=False):
        """
        Delete a style.

        Args:
          style_id (string): Name of the style to delete.
          purge (bool, optional): Purge if True.
          recurse (bool, optional): Delete recursively if True.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.

        Returns:
          (dict): Response dictionary
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
                             'error': e.message}

            self._handle_debug(response_dict, debug)
            return response_dict

    def get_layer_as_wfs(self, layer_id, **kwargs):
        """
        Get a layer as a WFS service

        Args:
          layer_id (string): Identifier of the dataset to retrieve.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:

          (str): WFS Query URL
        """
        return NotImplemented
    
    def get_layer_as_wms(self, layer_id, **kwargs):
        """
        Get a layer as a WMS service

        Args:
          layer_id (string): Identifier of the dataset to retrieve.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:

          (str): WMS Query URL
        """
        return NotImplemented

    def get_layer_as_wcs(self, layer_id, **kwargs):
        """
        Get a layer as a WCS service

        Args:
          layer_id (string): Identifier of the dataset to retrieve.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:

          (str): WCS Query URL
        """
        return NotImplemented

