import requests

from ..base import DatasetEngine


class HydroShareDatasetEngine(DatasetEngine):
    """
    Definition for HydroShare Dataset Engine objects.
    """

    @property
    def type(self):
        """
        HydroShare Dataset Engine Type
        """
        return 'HydroShare'

    def _prepare_request(self, method, data_dict=None, file=None, apikey=None):
        """
        Preprocess the parameters for HydroShare API call.

        Args:
            method (string): The HydroShare API method (action) to call (e.g.: 'resource_show').
            data_dict (dict, optional): Dictionary of method (action) arguments.
            file (dict): Dictionary of file objects to upload.
            apikey (string): The HydroShare API key to use for authorization.

        Returns:
            tuple: url, data_dict, headers
        """
        pass

    @staticmethod
    def _execute_request(url, data, headers, file=None):
        """
        Execute the request using the requests module.

        Args:
          url (string): The request url, usually the 'url' returned by '_prepare_request'.
          data (dict): Key value parameters to send with request, usually the 'data_dict' returned by '_prepare_request'.
          headers (dict): Key value parameters to include in the headers, usually the 'headers' returned by '_prepare_request'.
          file (dict): Dictionary containing file to upload. See: http://docs.python-requests.org/en/latest/user/quickstart/#post-a-multipart-encoded-file

        Returns:
          tuple: status_code, response
        """
        r = requests.post(url, data=data, headers=headers, files=file)
        return r.status_code, r.text

    @staticmethod
    def _parse_response(status, response, console=False):
        """
        Parse the response and check for errors.

        Args:
          status (int): Status code of the response.
          response (string): Response string.
          console (bool, optional): Pretty print the response to the console for debugging. Defaults to False.

        Returns:
          dict: response parsed into a dictionary or raises appropriate error.
        """
        pass

    def search_datasets(self, query, console=False, **kwargs):
        """
        Search HydroShare resources that match a query.

        Args:
          query (dict): Key value pairs representing field and values to search for.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def search_resources(self, query, console=False, **kwargs):
        """
        Search HydroShare files that match a query.

        Args:
          query (dict): Key value pairs representing field and values to search for.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def list_datasets(self, with_resources=False, console=False, **kwargs):
        """
        List HydroShare resources

        Args:
          with_resources (bool, optional): Return a list of dataset dictionaries. Defaults to False.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          list: A list of dataset names or a list of dataset dictionaries if with_resources is true.
        """
        pass

    def get_dataset(self, dataset_id, console=False, **kwargs):
        """
        Retrieve HydroShare resource

        Args:
          dataset_id (string): The id or name of the dataset to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def get_resource(self, resource_id, console=False, **kwargs):
        """
        Retrieve HydroShare file

        Args:
          resource_id (string): The id of the resource to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def create_dataset(self, name, console=False, **kwargs):
        """
        Create a new HydroShare resource.

        Args:
          name (string): The id or name of the resource to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def create_resource(self, dataset_id, url=None, file=None, console=False, **kwargs):
        """
        Create a new HydroShare file

        Args:
          dataset_id (string): The id or name of the dataset to to which the resource will be added.
          url (string, optional): URL for the resource that will be added to the dataset.
          file (string, optional): Absolute path to a file to upload for the resource.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def update_dataset(self, dataset_id, console=False, **kwargs):
        """
        Update HydroShare resource

        Args:
          dataset_id (string): The id or name of the dataset to update.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def update_resource(self, resource_id, url=None, file=None, console=False, **kwargs):
        """
        Update HydroShare file

        Args:
          resource_id (string): The id of the resource that will be updated.
          url (string, optional): URL of the resource that will be added to the dataset.
          file (string, optional): Absolute path to a file to upload for the resource.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def delete_dataset(self, dataset_id, console=False, **kwargs):
        """
        Delete HydroShare resource

        Args:
          dataset_id (string): The id or name of the dataset to delete.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass

    def delete_resource(self, resource_id, console=False, **kwargs):
        """
        Delete HydroShare file.

        Args:
          resource_id (string): The id of the resource to delete.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see HydroShare docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        pass