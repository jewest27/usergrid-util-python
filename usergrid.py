import json
import logging
import traceback
import requests
import time

__author__ = 'Jeff West @ ApigeeCorporation'

logger = logging.getLogger('usergrid')

class UsergridQuery:
    def __init__(self, url, operation='GET', headers=None, data=None):

        if not data:
            data = {}
        if not headers:
            headers = {}

        self.total_retrieved = 0
        self.logger = logging.getLogger(str(self.__class__))
        self.data = data
        self.headers = headers
        self.url = url
        self.operation = operation
        self.next_cursor = None
        self.entities = []
        self.count_retrieved = 0
        self._pos = 0
        self.last_response = None
        self.sleep_time = 5
        self.session = None

    def _get_next_response(self, attempts=0):

        if self.session is None:
            self.session = requests.Session()

        try:
            if self.operation == 'PUT':
                op = self.session.put
            elif self.operation == 'DELETE':
                op = self.session.delete
            else:
                op = self.session.get

            target_url = self.url

            if self.next_cursor is not None:

                if '?' in target_url:
                    delim = '&'
                else:
                    delim = '?'

                target_url = '%s%scursor=%s' % (self.url, delim, self.next_cursor)

            r = op(target_url, data=json.dumps(self.data), headers=self.headers)

            if r.status_code == 200:
                r_json = r.json()
                self.logger.info('Retrieved [%s] entities' % len(r_json.get('entities', [])))
                return r_json

            else:
                if attempts < 300 / self.sleep_time:
                    self.logger.info('URL=[%s], response: %s' % (target_url, r.text))
                    self.logger.warning('Sleeping %s after HTTP [%s] for retry' % (r.status_code, self.sleep_time))
                    time.sleep(self.sleep_time)

                    if r.status_code >= 500 or r.status_code == 401:
                        return self._get_next_response(attempts=attempts + 1)

                    elif 400 <= r.status_code < 500:
                        raise SystemError('HTTP [%s] on attempt to get next page for url=[%s], will not retry: %s' % (
                            r.status_code, target_url, r.text))

                else:
                    raise SystemError('Unable to get next response after %s attempts' % attempts)

        except:
            print traceback.format_exc()

    def next(self):

        if self.last_response is None:
            logger.info('getting first page, url=[%s]' % self.url)
            self._process_next_page()

        elif self._pos >= len(self.entities) > 0 and self.next_cursor is not None:
            logger.info('getting next page, count=[%s] url=[%s], cursor=[%s]' % (
                self.count_retrieved, self.url, self.next_cursor))
            self._process_next_page()

        if self._pos < len(self.entities):
            response = self.entities[self._pos]
            self._pos += 1
            return response

        raise StopIteration

    def __iter__(self):
        return self

    def _process_next_page(self, attempts=0):
        api_response = self._get_next_response()

        self.last_response = api_response
        self.entities = api_response.get('entities', [])
        self.next_cursor = api_response.get('cursor')
        self._pos = 0
        self.count_retrieved += len(self.entities)

        if self.next_cursor is None:
            logger.warning('no cursor in response. Total=[%s] url=[%s]' % (self.count_retrieved, self.url))
