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
                response_dict['result'] = e.message
        else:
            # Update response dictionary
            response_dict['success'] = False
            response_dict['result'] = 'GeoServer object does not exist: "{0}".'.format(identifier)

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

    @staticmethod
    def _transcribe_geoserver_object(gs_object):
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

        return object_dictionary

    def list_resources(self, with_properties=False, debug=False, **kwargs):
        """
        List all resources available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of resource dictionaries instead of a list of resource names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of resources
        catalog = self._get_geoserver_catalog_object()
        resource_objects = catalog.get_resources()

        if not with_properties:
            resource_names = []

            for resource_object in resource_objects:
                resource_names.append(resource_object.name)

            # Handle the debug and return
            self._handle_debug(resource_names, debug)
            return resource_names

        # Handle the debug and return
        resource_dicts = self._transcribe_geoserver_objects(resource_objects)
        self._handle_debug(resource_dicts, debug)
        return resource_dicts

    def list_layers(self, with_properties=False, debug=False, **kwargs):
        """
        List all layers available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of layer dictionaries instead of a list of layer names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layers
        catalog = self._get_geoserver_catalog_object()
        layer_objects = catalog.get_layers()

        # Compile list layer names only
        if not with_properties:
            layer_names = []

            for layer_object in layer_objects:
                layer_names.append(layer_object.name)

            # Handle the debug and return
            self._handle_debug(layer_names, debug)
            return layer_names

        # Handle the debug and return
        layer_dicts = self._transcribe_geoserver_objects(layer_objects)
        self._handle_debug(layer_dicts, debug)
        return layer_dicts

    def list_layer_groups(self, with_properties=False, debug=False, **kwargs):
        """
        List all layer groups available from the spatial dataset service.

        Args:
          with_properties (bool, optional): Return list of layer group dictionaries instead of a list of layer group names.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        layer_group_objects = catalog.get_layergroups()

        if not with_properties:
            layer_group_names = []

            for layer_group_object in layer_group_objects:
                layer_group_names.append(layer_group_object.name)

            # Handle the debug and return
            self._handle_debug(layer_group_names, debug)
            return layer_group_names

        # Handle the debug and return
        layer_group_dicts = self._transcribe_geoserver_objects(layer_group_objects)
        self._handle_debug(layer_group_dicts, debug)
        return layer_group_dicts

    def get_resource(self, resource_id, store=None, workspace=None, debug=False, **kwargs):
        """
        Retrieve a resource object.

        Args:
          resource_id (string): Name of the resource to retrieve.
          store (string, optional): Name of the store  from which to get the resource.
          workspace (string, optional): Name of workspace from which to get the resource.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get resource
        resource = catalog.get_resource(name=resource_id, store=store, workspace=workspace)
        resource_dict = self._transcribe_geoserver_object(resource)

        # Handle the debug and return
        self._handle_debug(resource_dict, debug)
        return resource_dict

    def get_layer(self, layer_id, debug=False, **kwargs):
        """
        Retrieve a layer object.

        Args:
          layer_id (string): Name of the layer to retrieve.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get resource
        layer = catalog.get_layer(name=layer_id)
        layer_dict = self._transcribe_geoserver_object(layer)

        # Handle the debug and return
        self._handle_debug(layer_dict, debug)
        return layer_dict

    def get_layer_group(self, layer_group_id, debug=False, **kwargs):
        """
        Retrieve a layer group object.

        Args:
          layer_group_id (string): Identifier of the layer to retrieve.
          debug (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get resource
        layer_group = catalog.get_layergroup(name=layer_group_id)
        layer_group_dict = self._transcribe_geoserver_object(layer_group)

        # Handle the debug and return
        self._handle_debug(layer_group_dict, debug)
        return layer_group_dict

    def create_resource(self, layer_id, url=None, file=None, **kwargs):
        pass

    def create_shapefile_resource(self, layer_id, shapefile_base, overwrite=False, charset=None, debug=True):
        """
        Create a new resource.

        Args:
          dataset_id (string): Identifier of the dataset to which the resource will be added.
          url (string, optional): URL of resource to associate with resource.
          file (string, optional): Path of file to upload as resource.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Process identifier
        workspace, name = self._process_identifier(layer_id)

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

    def create_layer(self, layer_id, debug=False):
        """
        Create a new layer.

        Args:
          name (string): Name of the dataset to create.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()


    def create_layer_group(self, layer_group_id, layers, styles, bounds=None, debug=False):
        """
        Create a new resource.

        Args:
          dataset_id (string): Identifier of the dataset to which the resource will be added.
          url (string, optional): URL of resource to associate with resource.
          file (string, optional): Path of file to upload as resource.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

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

        self._handle_debug(response_dict, debug)
        return response_dict

    def update_resource(self, resource_id, store=None, workspace=None, debug=False, **kwargs):
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

        # Get resource
        resource = catalog.get_resource(name=resource_id, store=store, workspace=workspace)

        # Make the changes
        updated_resource = self._apply_changes_to_gs_object(kwargs, resource)

        # Save the changes
        catalog.save(updated_resource)

        # Return the updated resource dictionary
        resource_dict = self._transcribe_geoserver_object(updated_resource)
        self._handle_debug(resource_dict, debug)
        return resource_dict

    def update_layer(self, layer_id, debug=False, **kwargs):
        """
        Update an existing layer.

        Args:
          layer_id (string): Identifier of the dataset to update.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get resource
        layer = catalog.get_layer(name=layer_id)

        # Apply changes from kwargs
        updated_layer = self._apply_changes_to_gs_object(kwargs, layer)

        # Save the changes
        catalog.save(updated_layer)

        # Return the updated resource dictionary
        layer_dict = self._transcribe_geoserver_object(updated_layer)
        self._handle_debug(layer_dict, debug)
        return layer_dict

    def update_layer_group(self, layer_group_id, debug=False, **kwargs):
        """
        Update an existing layer. If modifying the layers, ensure the number of layers
        and the number of styles are the same.

        Args:
          layer_group_id (string): Identifier of the dataset to update.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

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
        self._handle_debug(layer_group_dict, debug)
        return layer_group_dict

    def delete_resource(self, resource_id, purge=False, recurse=False, debug=False):
        """
        Delete a resource.

        Args:
          resource_id (string): Name of the resource to delete.

        Returns:
          (dict): Response dictionary
        """
        # Get a GeoServer catalog object and query for list of layer groups
        catalog = self._get_geoserver_catalog_object()

        # Get resource
        resource = catalog.get_resource(name=resource_id)

        # Handle delete
        return self._handle_delete(identifier=resource_id, gs_object=resource, purge=purge,
                                   recurse=recurse, debug=debug)

    def delete_layer(self, layer_id, purge=False, recurse=False, debug=False):
        """
        Delete a layer.

        Args:
          layer_id (string): Name of the layer to delete.

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

