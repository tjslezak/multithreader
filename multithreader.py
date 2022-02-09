from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import logging
import threading
from requests import Session


class MultiThreader:
    '''Multi-thread requests and process responses
        Args:
            mapping --> dictionary -> {key: unique_id, value: url}
            parser --> function -> processes data from response
            headers --> dictionary -> session-level HTML headers
            max_workers --> integer -> max number of ThreadPoolExecutor workers
            response_type --> 'json', 'text', or 'content' ('content' for bytes)
    '''
    # Here you can define the headers valid across all instances, combines with instance specific headers
    headers = {}

    def __init__(self, mapping, parser=None, headers=dict(), max_workers=30, response_type='text'):
        '''Initialize instance values'''
        self.logger = logging.getLogger(__name__)
        self.mapping = mapping
        self.parser = parser
        self.headers = headers
        self.headers.update(MultiThreader.headers)
        self.max_workers = max_workers
        self.response_type = response_type
        self._thread_local = threading.local()
        self.data = defaultdict()

    def _get_session(self):
        '''Helper function to make requests in parallel via the requests module.
            Returns: self._thread_local.session, requests Session
        '''
        if not hasattr(self._thread_local, "session"):
            self._thread_local.session = Session()
            self._thread_local.session.headers.update(self.headers)
            return self._thread_local.session

    def collect_response(self, arg_tuple):
        '''Applies parsing function to raw request data.
            Args:
                arg_tuple -> (unique_id, url)
            Populates:
                self.data -> defaultdict(unique_id: self.parser(response))
        '''
        session = self._get_session()
        unique_id, url = arg_tuple
        with session.get(url) as response:
            # Process response as either text or bytes (content)
            if self.response_type in ('text', 'content'):
                data = response.__getattribute__(self.response_type)
            # Process response as json
            elif self.response_type == 'json':
                data = response.json()
            else:
                data = response
            # If no parser was supplied, store raw response
            if self.parser is None:
                self.data[unique_id] = data
                # If parser supplied, use it to process the data before storage
            else:
                self.data[unique_id] = self.parser(data)

    def run(self):
        '''Multi-thread requests in parallel
            Args:
                self.collect_response -> function
                self.mapping -> dict -> {key: uniqueID, value: url}
            Returns:
                self.data
        '''
        with ThreadPoolExecutor(max_workers=self.max_workers) as executioner:
            executioner.map(self.collect_response, self.mapping.items())

        return self.data
