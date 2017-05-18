import os
import json
import pprint
import warnings

import requests
from requests_toolbelt import MultipartEncoder

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
        if file:
            data.update(file)
            m = MultipartEncoder(fields=data)
            headers['Content-Type'] = m.content_type
            r = requests.post(url, data=m, headers=headers)
        else:
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

        except Exception as e:
            print(e)
            print('Status Code {0}: {1}'.format(status, response.encode('utf-8')))
            return None

    def execute_api_method(self, method, console=False, file=None, apikey=None, **kwargs):
        # Execute
        url, data, headers = self._prepare_request(method=method, file=file, apikey=apikey, data_dict=kwargs)
        status, response = self._execute_request(url=url, data=data, headers=headers, file=file)

        return self._parse_response(status, response, console)

    def _get_query_params(self, query_dict):
        """
        Assembles query string from python dictionary
        """
        query_terms = []
        if len(query_dict.keys()) > 1:
            for key, value in query_dict.iteritems():
                query_terms.append('{0}:{1}'.format(key, value))
        else:
            for key, value in query_dict.iteritems():
                query_terms = '{0}:{1}'.format(key, value)
        return query_terms

    def search_datasets(self, query=None, filtered_query=None, console=False, **kwargs):
        """
        Search CKAN datasets that match a query.

        Wrapper for the CKAN search_datasets API method. See the CKAN API docs for this methods to see applicable
        options (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          query (dict, optional if filtered_query set): Key value pairs representing field and values to search for.
          filtered_query (dict, optional if filtered_query set): Key value pairs representing field and values to search for.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        if not query and not filtered_query:
            raise Exception("Need query or filtered_query to proceed ...")

        # Assemble data dictionary
        data = kwargs

            
        # Assemble the query parameters
        if query:
            data['q'] = self._get_query_params(query)

        if filtered_query:
            data['fq'] = self._get_query_params(filtered_query)

        # Execute
        method = 'package_search'
        return self.execute_api_method(method=method, console=console, **data)

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
        data['query'] = self._get_query_params(query)

        # Special error
        error_409 = 'HTTP ERROR 409: Ensure query fields are valid and try again.'

        # Execute
        method='resource_search'
        return self.execute_api_method(method=method, console=console, **data)

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
        # Assemble data dictionary
        data = kwargs

        # Execute API Method
        if not with_resources:
            method='package_list'
        else:
            method='current_package_list_with_resources'

        return self.execute_api_method(method=method, console=console, **data)

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
        method='package_show'
        return self.execute_api_method(method=method, console=console, **data)

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
        method='resource_show'
        return self.execute_api_method(method=method, console=console, **data)

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
        method='package_create'
        return self.execute_api_method(method=method, console=console, **data)

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
        data= kwargs
        data['package_id'] = dataset_id

        if url:
            data['url'] = url
        else:
            data['url'] = ''

        # Default naming convention
        if 'name' not in data and file:
            data['name'] = os.path.basename(file)

        # Prepare file
        if file:
            if not os.path.isfile(file):
                raise IOError('The file "{0}" does not exist.'.format(file))
            else:
                filename, extension = os.path.splitext(file)
                upload_file_name = data['name']
                if not upload_file_name.endswith(extension):
                    upload_file_name += extension
                file = {'upload': (upload_file_name, open(file, 'r'))}

        # Execute
        method='resource_create'
        return self.execute_api_method(method=method, console=console, file=file, **data)

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
        method='package_update'
        return self.execute_api_method(method=method, console=console, **data)

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
        data = kwargs
        data['id'] = resource_id

        if url:
            data['url'] = url

        # Default naming convention
        if 'name' not in data and file:
            data['name'] = os.path.basename(file)

        # Prepare file
        if file:
            if not os.path.isfile(file):
                raise IOError('The file "{0}" does not exist.'.format(file))
            else:
               file = {'upload': open(file)}

        # if not url and not file:
        if not 'url' in data:
            result = self.get_resource(resource_id)
            if result['success']:
                resource = result['result']
                data['url'] = resource['url']

        # Execute
        method='resource_update'
        return self.execute_api_method(method=method, console=console, file=file, **data)

    def delete_dataset(self, dataset_id, console=False, file=None, **kwargs):
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
        method='package_delete'
        return self.execute_api_method(method=method, console=console, file=file, **data)

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
        method='resource_delete'
        return self.execute_api_method(method=method, console=console, **data)

    def download_dataset(self, dataset_id, location=None, console=False, **kwargs):
        """
        Downloads all resources in a dataset

        Description

        Args:
            dataset_id (string): The id of the dataset to download.
            location (string, optional): Path to the location for the resource to be downloaded. Default is a subdirectory in the current directory named after the dataset.
            console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
            **kwargs: Any number of optional keyword arguments to pass to the get_dataset method (see CKAN docs).

        Returns:
            A list of the files that were downloaded.
        """
        result = self.get_dataset(dataset_id, console=console, **kwargs)
        if result['success']:
            dataset = result['result']

            location = location or dataset['name']

            downloaded_resources = []
            for resource in dataset['resources']:
                downloaded_resource = self._download_resource(resource, location)
                downloaded_resources.append(downloaded_resource)

            return downloaded_resources
        else:
            raise Exception(str(result))#TODO raise an error stating that dataset doesn't exist

    def download_resouce(self, resource_id, location=None, local_file_name=None, console=False, **kwargs):
        """
        Deprecated alias for download_resource method for backwards compatibility (the old method was misspelled).

        Description

        Args:
            resource_id (string): The id of the resource to download.
            location (string, optional): Path to the location for the resource to be downloaded. Defaults to current directory.
            local_file_name (string, optional): Name for downloaded file.
            console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
            **kwargs: Any number of optional keyword arguments to pass to the get_resource method (see CKAN docs).

        Returns:
            Path and name of the downloaded file.
        """
        warnings.warn(
            "This method has been deprecated because it was misspelled. Use download_resource instead.",
            DeprecationWarning
        )
        self.download_resource(
            resource_id=resource_id,
            location=location,
            local_file_name=local_file_name,
            console=console,
            **kwargs
        )

    def download_resource(self, resource_id, location=None, local_file_name=None, console=False, **kwargs):
        """
        Download a resource from a resource id

        Description

        Args:
            resource_id (string): The id of the resource to download.
            location (string, optional): Path to the location for the resource to be downloaded. Defaults to current directory.
            local_file_name (string, optional): Name for downloaded file.
            console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
            **kwargs: Any number of optional keyword arguments to pass to the get_resource method (see CKAN docs).

        Returns:
            Path and name of the downloaded file.
        """
        result = self.get_resource(resource_id, console=console, **kwargs)
        if result['success']:
            resource = result['result']
            downloaded_resource = self._download_resource(resource, location, local_file_name)

            return downloaded_resource
        else:
            raise Exception(str(result))#TODO raise an error stating that dataset doesn't exist


    def _download_resource(self, resource, location=None, local_file_name=None):
        """
        Download a resource from the resource meta-data dictionary
        """

        # create filename with extension
        if not local_file_name:
            local_file_name = resource['name'] or resource['id']
            local_file_name = '.'.join((local_file_name, resource['format']))

        # ensure that the location exists
        if location:
            try:
                os.makedirs(location)
            except OSError as e:
                pass
        else:
            location = './'


        local_file = os.path.join(location, local_file_name)
        url = resource['url']

        # download resource
        try:
            r = requests.get(url, stream=True)
            with open(local_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()
        except Exception as e:
            print(e)

        return local_file

    def validate(self):
        """
        Validate CKAN dataset engine. Will throw an error if not valid.
        """
        # Strip off the '/action' or '/action/' portion of the endpoint URL
        if self.endpoint[-1] == '/':
            api_endpoint = self.endpoint[:-8]
        else:
            api_endpoint = self.endpoint[:-7]

        try:
            r = requests.get(api_endpoint)

        except requests.exceptions.MissingSchema:
            raise AssertionError('The URL "{0}" provided for the CKAN dataset service endpoint is invalid.'.format(self.endpoint))

        except:
            raise

        if r.status_code != 200:
            raise AssertionError('The URL "{0}" is not a valid endpoint for a CKAN dataset service.'.format(self.endpoint))

        if 'version' not in r.json():
            raise AssertionError('The URL "{0}" is not a valid endpoint for a CKAN dataset service.'.format(self.endpoint))