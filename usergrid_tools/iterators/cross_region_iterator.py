__author__ = 'Jeff West @ ApigeeCorporation'

from Queue import Empty
import argparse
import json
import time
import logging
import sys
from multiprocessing import Process, JoinableQueue
import datetime
import requests
import traceback
from logging.handlers import RotatingFileHandler
import urllib3
import urllib3.contrib.pyopenssl

urllib3.disable_warnings()
urllib3.contrib.pyopenssl.inject_into_urllib3()


# This was used to force a sync of C* across the regions.  The idea is to query entities from
# a region where they exist using QL.  Then, iterate over the results and do a GET by UUID
# in the region where the entities are 'missing'.
#
# In order for this to be successful the readcl in the "GET by UUID" region or target region
# must be set to 'ALL' - this will force a repair across the cluster.
#
# It is recommended to have the target tomcat out of the ELB for a customer.  Ideally,
# you should spin up another Tomcat, leaving 2+ in the ELB for a given customer.


logger = logging.getLogger('CrossRegionRepair')

token_url_template = "{protocol}://{host}/management/token"
org_management_url_template = "{protocol}://{host}/management/organizations/{org}/applications?access_token={access_token}"
org_url_template = "{protocol}://{host}/{org}?access_token={access_token}"
app_url_template = "{protocol}://{host}/{org}/{app}?access_token={access_token}"
collection_url_template = "{protocol}://{host}/{org}/{app}/{collection}?access_token={access_token}"
collection_query_url_template = "{protocol}://{host}/{org}/{app}/{collection}?ql={ql}&access_token={access_token}&limit={limit}"
get_entity_url_template = "{protocol}://{host}/{org}/{app}/{collection}/{uuid}?access_token={access_token}&connections=none"
put_entity_url_template = "{protocol}://{host}/{org}/{app}/{collection}/{uuid}?access_token={access_token}"

# config can be loaded from a file
config = {}

# config = {
#     "regions": {
#         "us_west": {
#             "protocol": "http",
#             "host": "rut040wo:8080"
#         },
#         "us_east": {
#             "protocol": "http",
#             "host": "rut154ea:8080"
#         },
#         "eu_west": {
#             "protocol": "http",
#             "host": "localhost:9090"
#         }
#     },
#     "management_region_id": "us_west",
#     "query_region_id": "us_west",
#     "get_region_ids": [
#         "us_east"
#     ]
# }

session_map = {}


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './cross-region-repair.log'
    log_formatter = logging.Formatter(fmt='%(asctime)s | %(name)s | %(processName)s | %(levelname)s | %(message)s',
                                      datefmt='%m/%d/%Y %I:%M:%S %p')

    rotating_file = logging.handlers.RotatingFileHandler(filename=log_file_name,
                                                         mode='a',
                                                         maxBytes=204857600,
                                                         backupCount=10)
    rotating_file.setFormatter(log_formatter)
    rotating_file.setLevel(logging.INFO)

    root_logger.addHandler(rotating_file)
    root_logger.setLevel(logging.INFO)

    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    if stdout_enabled:
        stdout_logger = logging.StreamHandler(sys.stdout)
        stdout_logger.setFormatter(log_formatter)
        stdout_logger.setLevel(logging.INFO)
        root_logger.addHandler(stdout_logger)


class Worker(Process):
    def __init__(self, queue, handler_function):
        super(Worker, self).__init__()
        logger.warning('Creating worker!')
        self.queue = queue
        self.handler_function = handler_function

    def run(self):

        logger.info('starting run()...')
        keep_going = True

        count_processed = 0
        count_error = 0

        while keep_going:
            empty_count = 0

            try:
                org, app, collection, entity = self.queue.get(timeout=600)
                logger.debug('Task: org=[%s] app=[%s] collection=[%s] entity=[%s]' % (org, app, collection, entity))

                if self.handler_function is not None:
                    processed = self.handler_function(org=org,
                                                      app=app,
                                                      collection=collection,
                                                      entity=entity,
                                                      counter=count_processed)

                    if processed:
                        count_processed += 1
                        logger.info('Processed count=[%s] SUCCESS uuid/name = %s / %s' % (
                            count_processed, entity.get('uuid'), entity.get('name')))
                    else:
                        count_error += 1
                        logger.error('Processed count=[%s] ERROR uuid/name = %s / %s' % (
                            count_error, entity.get('uuid'), entity.get('name')))

                self.queue.task_done()

            except KeyboardInterrupt, e:
                raise e

            except Empty:
                logger.warning('EMPTY!')
                empty_count += 1
                if empty_count > 30:
                    keep_going = False

        logger.warning('WORKER DONE!')


def wait_for(threads, sleep_time=3000):
    count_alive = 1

    while count_alive > 0:
        count_alive = 0

        for t in threads:

            if t.is_alive():
                count_alive += 1

        if count_alive > 0:
            logger.warning('Waiting for [%s] processes to finish' % count_alive)
            time.sleep(sleep_time)


def parse_args():
    DEFAULT_WORKERS = 16
    DEFAULT_TOKEN_TTL = 25200000

    parser = argparse.ArgumentParser(description='Usergrid Cross-Region Repair Script')

    parser.add_argument('-o', '--org',
                        help='The org to iterate',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='The org to iterate',
                        action='append',
                        default=[])

    parser.add_argument('-c', '--collection',
                        help='The org to iterate',
                        action='append',
                        default=[])

    parser.add_argument('-p', '--password',
                        help='The Password for the token request',
                        type=str,
                        required=True)

    parser.add_argument('-u', '--username',
                        help='The Username for the token request',
                        type=str,
                        required=True)

    parser.add_argument('-w', '--workers',
                        help='The Password for the token request',
                        type=int,
                        default=DEFAULT_WORKERS)

    parser.add_argument('--ttl',
                        help='The TTL for the token request',
                        type=int,
                        default=DEFAULT_TOKEN_TTL)

    parser.add_argument('-l', '--limit',
                        help='The global limit for QL requests',
                        type=int,
                        default=DEFAULT_WORKERS * 3)

    parser.add_argument('-f', '--config',
                        help='The file from which to load the configuration',
                        type=str)

    my_args = parser.parse_args(sys.argv[1:])

    return vars(my_args)


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

        # if api_response.status_code != 200:
        #     error_message = 'HTTP [%s] getting next page: %s' % (api_response.status_code, api_response.text)
        #     logger.error(error_message)
        #
        #     if api_response.status_code >= 500:
        #         logger.warn('Sleeping for 5 before retry on URL=[%s]' % self.url)
        #         time.sleep(5)
        #         return self._process_next_page(attempts=attempts + 1)
        #
        #     else:
        #         raise Exception(error_message)

        self.entities = api_response.get('entities', [])
        self.next_cursor = api_response.get('cursor')
        self._pos = 0
        self.count_retrieved += len(self.entities)

        if self.next_cursor is None:
            logger.warning('no cursor in response. Total=[%s] url=[%s]' % (self.count_retrieved, self.url))


def get_by_UUID(org, app, collection, entity, counter, attempts=0):
    response = False

    if attempts >= 10:
        return False

    for region_id in config.get('get_region_ids', []):
        url_data = config.get('regions', {}).get(region_id)

        url = get_entity_url_template.format(collection=collection,
                                             app=app,
                                             uuid=entity.get('uuid'),
                                             org=org,
                                             access_token=config['access_token'],
                                             **url_data)

        logger.info('GET [%s]: %s' % ('...', url))

        session = session_map[region_id]

        while not response:

            try:
                r = session.get(url)

                if r.status_code != 200:
                    logger.error('GET [%s] (%s): %s' % (r.status_code, r.elapsed, url))
                    logger.warning('Sleeping for 5 on connection retry...')

                    return get_by_UUID(org, app, collection, entity, counter, attempts=attempts + 1)

                else:
                    logger.info('GET [%s] (%s): %s' % (r.status_code, r.elapsed, url))
                    response = True

                if counter % 10 == 0:
                    logger.info('COUNTER=[%s] time=[%s] GET [%s]: %s' % (counter,
                                                                         r.elapsed,
                                                                         r.status_code,
                                                                         url))
            except:
                logger.error(traceback.format_exc())
                logger.error('EXCEPTION on GET [...] (...): %s' % url)
                response = False
                logger.warning('Sleeping for 5 on connection retry...')
                time.sleep(5)

    return response


def init(args):
    global config

    if args.get('config') is not None:
        config_filename = args.get('config')

        logger.warning('Using config file: %s' % config_filename)

        try:
            with open(config_filename, 'r') as f:
                parsed_config = json.load(f)
                logger.warning('Updating config with: %s' % parsed_config)
                config.update(parsed_config)
        except:
            print traceback.format_exc()

    for region_id, region_data in config.get('regions', {}).iteritems():
        session_map[region_id] = requests.Session()


def main():
    global config

    args = parse_args()
    init(args)

    management_region_id = config.get('management_region_id', '')
    management_region = config.get('regions', {}).get(management_region_id)

    query_region_id = config.get('query_region_id', '')
    query_region = config.get('regions', {}).get(query_region_id)

    start = datetime.datetime.now()

    queue = JoinableQueue()

    logger.warning('Starting workers...')
    init_logging()

    token_request = {
        'grant_type': 'password',
        'username': args.get('username'),
        'ttl': args.get('ttl')
    }

    url = token_url_template.format(**management_region)

    logger.info('getting token with url=[%s] data=[%s]' % (url, token_request))

    token_request['password'] = args.get('password')

    r = requests.post(url, data=json.dumps(token_request))

    if r.status_code != 200:
        logger.critical('did not get access token! response: %s' % r.json())
        exit(-1)

    logger.info(r.json())

    config['access_token'] = r.json().get('access_token')

    org_mgmt_url = org_management_url_template.format(org=args.get('org'),
                                                      access_token=config['access_token'],
                                                      **management_region)
    logger.info(org_mgmt_url)

    session = session_map[management_region_id]

    r = session.get(org_mgmt_url)
    logger.info(r.json())
    logger.info('starting [%s] workers...' % args.get('workers'))
    workers = [Worker(queue, get_by_UUID) for x in xrange(args.get('workers'))]
    [w.start() for w in workers]

    try:
        org_app_data = r.json().get('data')

        logger.info(org_app_data)

        apps_to_process = config.get('app', [])
        collections_to_process = config.get('collection', [])

        for org_app, app_uuid in org_app_data.iteritems():
            parts = org_app.split('/')
            app = parts[1]

            if len(apps_to_process) > 0 and app not in apps_to_process:
                logger.info('Skipping app/uuid: %s/%s' % (org_app, app_uuid))
                continue

            logger.info('app UUID: %s' % app_uuid)

            url = app_url_template.format(app=app,
                                          org=args.get('org'),
                                          access_token=config['access_token'],
                                          **management_region)

            logger.info('GET [...]: %s' % url)
            session = session_map[management_region_id]
            r = session.get(url)

            for collection_name in r.json().get('entities', [{}])[0].get('metadata', {}).get('collections', {}):

                if collection_name in ['events']:
                    continue

                elif len(collections_to_process) > 0 and collection_name not in collections_to_process:
                    logger.info('skipping collection=%s' % collection_name)
                    continue

                logger.info('processing collection=%s' % collection_name)

                url = collection_query_url_template.format(ql='select * order by created asc',
                                                           collection=collection_name,
                                                           org=args['org'],
                                                           app=app,
                                                           limit=args['limit'],
                                                           access_token=config['access_token'],
                                                           **query_region)

                q = UsergridQuery(url)
                counter = 0

                for x, e in enumerate(q):
                    counter += 1
                    queue.put((args['org'], app, collection_name, e))

                logger.info('collection=%s, count=%s' % (collection_name, counter))

    except KeyboardInterrupt:
        [w.terminate() for w in workers]

    logger.warning('Waiting for workers to finish...')
    wait_for(workers)

    finish = datetime.datetime.now()
    logger.warning('Done!  Took: %s ' % (finish - start))


main()
