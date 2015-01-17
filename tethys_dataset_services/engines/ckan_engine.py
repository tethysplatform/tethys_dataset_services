import os
import json
import pprint
import requests

from ..base import DatasetEngine


class CkanDatasetEngine(DatasetEngine):
    """
    Definition for CKAN Dataset Engine objects.
    """

    @property
    def type(self):
        """
        CKAN Dataset Engine Type
        """
        return 'CKAN'

    def _prepare_request(self, method, data_dict=None, file=None, apikey=None):
        """
        Preprocess the parameters for CKAN API call. This is derived from CKAN's API client which can be found here:
        https://github.com/ckan/ckanapi/tree/master/ckanapi/common.py

        Args:
            method (string): The CKAN API method (action) to call (e.g.: 'resource_show').
            data_dict (dict, optional): Dictionary of method (action) arguments.
            file (dict): Dictionary of file objects to upload.
            apikey (string): The CKAN API key to use for authorization.

        Returns:
            tuple: url, data_dict, headers
        """
        if not data_dict:
            data_dict = {}

        headers = {}

        if file:
            data_dict = dict((k.encode('utf-8'), v.encode('utf-8'))
                for (k, v) in data_dict.items())
        else:
            data_dict = json.dumps(data_dict).encode('ascii')
            headers['Content-Type'] = 'application/json'

        if apikey:
            apikey = str(apikey)
        else:
            apikey = str(self.apikey)

        headers['X-CKAN-API-Key'] = apikey
        headers['Authorization'] = apikey

        url = '/'.join((self.endpoint.rstrip('/'), method))

        return url, data_dict, headers

    @staticmethod
    def _execute_request(url, data, headers, file=None):
        """
        Execute the request using the requests module. See: https://github.com/ckan/ckanapi/tree/master/ckanapi/common.py

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
        try:
            parsed = json.loads(response)
            if console:
                if hasattr(parsed, 'get'):
                    if parsed.get('success'):
                        pprint.pprint(parsed)
                    else:
                        print('ERROR: {0}'.format(parsed['error']['message']))
            return parsed

        except:
            print('Status Code {0}: {1}'.format(status, response))
            return None

    def search_datasets(self, query, console=False, **kwargs):
        """
        Search CKAN datasets that match a query.

        Wrapper for the CKAN search_datasets API method. See the CKAN API docs for this methods to see applicable
        options (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          query (dict): Key value pairs representing field and values to search for.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs

        # Assemble the query parameters
        query_terms = []

        if len(query.keys()) > 1:
            for key, value in query.iteritems():
                query_terms.append('{0}:{1}'.format(key, value))
        else:
            for key, value in query.iteritems():
                query_terms = '{0}:{1}'.format(key, value)

        data['q'] = query_terms

        # Execute
        url, data, headers = self._prepare_request(method='package_search', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def search_resources(self, query, console=False, **kwargs):
        """
        Search CKAN resources that match a query.

        Wrapper for the CKAN search_resources API method. See the CKAN API docs for this methods to see applicable
        options (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          query (dict): Key value pairs representing field and values to search for.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs

        # Assemble the query parameters
        query_terms = []
        if len(query.keys()) > 1:
            for key, value in query.iteritems():
                query_terms.append('{0}:{1}'.format(key, value))
        else:
            for key, value in query.iteritems():
                query_terms = '{0}:{1}'.format(key, value)

        data['query'] = query_terms

        # Special error
        error_409 = 'HTTP ERROR 409: Ensure query fields are valid and try again.'

        # Execute
        url, data, headers = self._prepare_request(method='resource_search', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def list_datasets(self, with_resources=False, console=False, **kwargs):
        """
        List CKAN datasets.

        Wrapper for the CKAN package_list and current_package_list_with_resources API methods. See the CKAN API docs for
        these methods to see applicable options (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          with_resources (bool, optional): Return a list of dataset dictionaries. Defaults to False.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          list: A list of dataset names or a list of dataset dictionaries if with_resources is true.
        """
        # Execute API Method
        if not with_resources:
            url, data, headers = self._prepare_request(method='package_list', data_dict=kwargs)
            status, response = self._execute_request(url=url, data=data, headers=headers)

        else:
            url, data, headers = self._prepare_request(method='current_package_list_with_resources', data_dict=kwargs)
            status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def get_dataset(self, dataset_id, console=False, **kwargs):
        """
        Retrieve CKAN dataset

        Wrapper for the CKAN package_show API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs
        data['id'] = dataset_id

        # Execute
        url, data, headers = self._prepare_request(method='package_show', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def get_resource(self, resource_id, console=False, **kwargs):
        """
        Retrieve CKAN resource

        Wrapper for the CKAN resource_show API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          resource_id (string): The id of the resource to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs
        data['id'] = resource_id

        # Error message
        error_404 = 'HTTP ERROR 404: The resource could not be found. Check that the id provided is valid ' \
                    'and that the dataset service at {0} is running properly, then try again.'.format(self.endpoint)

        # Execute
        url, data, headers = self._prepare_request(method='resource_show', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def create_dataset(self, name, console=False, **kwargs):
        """
        Create a new CKAN dataset.

        Wrapper for the CKAN package_create API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          name (string): The id or name of the resource to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['name'] = name

        # Execute
        url, data, headers = self._prepare_request(method='package_create', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def create_resource(self, dataset_id, url=None, file=None, console=False, **kwargs):
        """
        Create a new CKAN resource.

        Wrapper for the CKAN resource_create API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to to which the resource will be added.
          url (string, optional): URL for the resource that will be added to the dataset.
          file (string, optional): Absolute path to a file to upload for the resource.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Validate file and url parameters (mutually exclusive)
        if url and file:
            raise IOError('The url and file parameters are mutually exclusive: use one, not both.')
        elif not url and not file:
            raise IOError('The url or file parameter is required, but do not use both.')

        # Assemble the data dictionary
        data_dict = kwargs
        data_dict['package_id'] = dataset_id

        if url:
            data_dict['url'] = url

        # Default naming convention
        if 'name' not in data_dict and file:
            data_dict['name'] = os.path.basename(file)

        # Prepare file
        if file:
            if not os.path.isfile(file):
                raise IOError('The file "{0}" does not exist.'.format(file))
            else:
                file = {'upload': open(file)}

        # Execute
        url, data, headers = self._prepare_request(method='resource_create', data_dict=data_dict, file=file)
        status, response = self._execute_request(url=url, data=data, headers=headers, file=file)

        return self._parse_response(status, response, console)

    def update_dataset(self, dataset_id, console=False, **kwargs):
        """
        Update CKAN dataset

        Wrapper for the CKAN package_update API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to update.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['id'] = dataset_id

        # Preserve the resources and tags if not included in parameters
        """
        Note: The default behavior of 'package_update' is to replace the resources and tags attributes with empty
              lists if they are not included in the parameter list... This is suboptimal, because the resources become
              disassociated with the dataset and float off into the ether. This behavior is modified in this method so
              that these properties are retained by default, unless included in the parameters that are being updated.
        """
        original_url, original_data, original_headers = self._prepare_request(method='package_show', data_dict=data)
        original_status, original_response = self._execute_request(url=original_url, data=original_data,
                                                                   headers=original_headers)
        original_result = self._parse_response(original_status, original_response)

        if original_result['success']:
            original_dataset = original_result['result']

            if 'resources' not in data:
                data['resources'] = original_dataset['resources']

            if 'tags' not in data:
                data['tags'] = original_dataset['tags']

        # Execute
        url, data, headers = self._prepare_request(method='package_update', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def update_resource(self, resource_id, url=None, file=None, console=False, **kwargs):
        """
        Update CKAN resource

        Wrapper for the CKAN resource_update API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          resource_id (string): The id of the resource that will be updated.
          url (string, optional): URL of the resource that will be added to the dataset.
          file (string, optional): Absolute path to a file to upload for the resource.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Validate file and url parameters (mutually exclusive)
        if url and file:
            raise IOError('The url and file parameters are mutually exclusive: use one, not both.')

        # Assemble the data dictionary
        data_dict = kwargs
        data_dict['id'] = resource_id

        if url:
            data_dict['url'] = url

        # Default naming convention
        if 'name' not in data_dict and file:
            data_dict['name'] = os.path.basename(file)

        # Prepare file
        if file:
            if not os.path.isfile(file):
                raise IOError('The file "{0}" does not exist.'.format(file))
            else:
               file = {'upload': open(file)}

        if not url and not file:
            result = self.get_resource(resource_id)
            if result['success']:
                resource = result['result']
                data_dict['url'] = resource['url']

        # Execute
        url, data, headers = self._prepare_request(method='resource_update', data_dict=data_dict, file=file)
        status, response = self._execute_request(url=url, data=data, headers=headers, file=file)

        return self._parse_response(status, response, console)

    def delete_dataset(self, dataset_id, console=False, **kwargs):
        """
        Delete CKAN dataset

        Wrapper for the CKAN package_delete API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to delete.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['id'] = dataset_id

        # Execute
        url, data, headers = self._prepare_request(method='package_delete', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)

    def delete_resource(self, resource_id, console=False, **kwargs):
        """
        Delete CKAN resource

        Wrapper for the CKAN resource_delete API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          resource_id (string): The id of the resource to delete.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['id'] = resource_id

        # Execute
        url, data, headers = self._prepare_request(method='resource_delete', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(status, response, console)
