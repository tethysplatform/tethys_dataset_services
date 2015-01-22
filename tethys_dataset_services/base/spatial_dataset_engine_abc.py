from abc import ABCMeta, abstractmethod, abstractproperty


class SpatialDatasetEngine:
    """
    The base definition for SpatialDatasetEngine objects.

    SpatialDatasetEngine objects are bound to a web API endpoint via the 'api_endpoint' property. Optionally,
    they can also be bound with an apikey or a username and password for operations that require
    authorization.

    Response Dictionary:

    All methods must return a response dictionary. This dictionary must have keys 'success' and either 'result'
    or 'error'. The value of the 'success' item should be a boolean indicating whether the operation was
    successful or not. If 'success' is True, then 'result' item will contain the resulting data. If 'success'
    is False, then 'error' should contain error information.
    """
    __metaclass__ = ABCMeta

    @property
    def endpoint(self):
        """
        API Endpoint (e.g.: www.example.com/api).
        """
        return self._endpoint

    @property
    def apikey(self):
        """
        API key for authorization.
        """
        return self._apikey

    @property
    def username(self):
        """
        Username for authorization.
        """
        return self._username

    @property
    def password(self):
        """
        Password for authorization.
        """
        return self._password

    @abstractproperty
    def type(self):
        """
        Stores a string representing the type of the dataset engine (e.g.: 'CKAN')
        """
        return NotImplemented

    def __init__(self, endpoint, apikey=None, username=None, password=None):
        """
        Default constructor for Dataset Engines.

        Args:
          api_endpoint (string): URL of the dataset service API endpoint (e.g.: www.host.com/api)
          apikey (string, optional): API key that will be used to authenticate with the dataset service.
          username (string, optional): Username that will be used to authenticate with the dataset service.
          password (string, optional): Password that will be used to authenticate with the dataset service.
        """
        self._endpoint = endpoint
        self._apikey = apikey
        self._username = username
        self._password = password

    def __repr__(self):
        """
        Representation of Dataset Engine object for debugging purposes.
        """
        return '<DatasetEngine type={0} endpoint={1}>'.format(self.type, self.endpoint)

    @abstractmethod
    def list_layers(self, **kwargs):
        """
        List all layers available from the spatial dataset service.

        Args:
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def list_resources(self, **kwargs):
        """
        List all resources available from the spatial dataset service.

        Args:
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def list_layer_groups(self, **kwargs):
        """
        List all layer groups available from the spatial dataset service.

        Args:
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def get_layer(self, layer_id, **kwargs):
        """
        Retrieve a layer object.

        Args:
          layer_id (string): Identifier of the layer to retrieve.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def get_layer_group(self, layer_group_id, **kwargs):
        """
        Retrieve a layer group object.

        Args:
          layer_id (string): Identifier of the layer to retrieve.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """

    @abstractmethod
    def get_resource(self, resource_id, **kwargs):
        """
        Retrieve a resource object.

        Args:
          resource_id (string): Identifier of the dataset to retrieve.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def create_layer(self, name, **kwargs):
        """
        Create a new layer.

        Args:
          name (string): Name of the dataset to create.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def create_resource(self, layer_id, url=None, file=None, **kwargs):
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
        return NotImplemented

    @abstractmethod
    def update_layer(self, layer_id, **kwargs):
        """
        Update an existing layer.

        Args:
          layer_id (string): Identifier of the dataset to update.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def update_resource(self, resource_id, url=None, file=None, **kwargs):
        """
        Update an existing resource.

        Args:
          resource_id (string): Identifier of the resource to update.
          url (string): URL of resource to associate with resource.
          file (string): Path of file to upload as resource.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def delete_layer(self, layer_id, **kwargs):
        """
        Delete a layer.

        Args:
          layer_id (string): Identifier of the dataset to delete.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
    def delete_resource(self, resource_id, **kwargs):
        """
        Delete a resource.

        Args:
          resource_id (string): Identifier of the resource to delete.
          **kwargs (kwargs, optional): Any number of additional keyword arguments.

        Returns:
          (dict): Response dictionary
        """
        return NotImplemented

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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