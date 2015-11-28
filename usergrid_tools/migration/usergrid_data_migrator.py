from Queue import Empty
import argparse
import json
import logging
import sys
from multiprocessing import Queue, Process
import requests
import traceback
from logging.handlers import RotatingFileHandler
import time
from usergrid import UsergridQuery
from requests.auth import HTTPBasicAuth

__author__ = 'Jeff West @ ApigeeCorporation'

logger = logging.getLogger('DataMigrator')


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './app_iterator.log'
    log_formatter = logging.Formatter(fmt='%(asctime)s | %(name)s | %(processName)s | %(levelname)s | %(message)s',
                                      datefmt='%m/%d/%Y %I:%M:%S %p')

    rotating_file = logging.handlers.RotatingFileHandler(filename=log_file_name,
                                                         mode='a',
                                                         maxBytes=2048576000,
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


config = {}

org_management_url_template = "{api_url}/management/organizations/{org}/applications?client_id={client_id}&client_secret={client_secret}"
org_url_template = "{api_url}/{org}?client_id={client_id}&client_secret={client_secret}"
app_url_template = "{api_url}/{org}/{app}?client_id={client_id}&client_secret={client_secret}"
collection_url_template = "{api_url}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}"
collection_query_url_template = "{api_url}/{org}/{app}/{collection}?ql={ql}&client_id={client_id}&client_secret={client_secret}&limit={limit}"
collection_graph_url_template = "{api_url}/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}&limit={limit}"
connection_query_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/{verb}?client_id={client_id}&client_secret={client_secret}&limit={limit}"
connection_create_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}/{verb}/{target_uuid}?client_id={client_id}&client_secret={client_secret}"
get_entity_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}&connections=none"

get_entity_url_with_connections_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"
put_entity_url_template = "{api_url}/{org}/{app}/{collection}/{uuid}?client_id={client_id}&client_secret={client_secret}"

user_credentials_url_template = "{api_url}/{org}/{app}/users/{uuid}/credentials?client_id={client_id}&client_secret={client_secret}"

ignore_collections = ['activities', 'queues', 'events']


class Worker(Process):
    def __init__(self, queue, handler_function):
        super(Worker, self).__init__()
        logger.debug('Creating worker!')
        self.queue = queue
        self.handler_function = handler_function

    def run(self):

        logger.info('starting run()...')
        keep_going = True

        count_processed = 0

        while keep_going:
            empty_count = 0
            try:
                app, collection_name, entity = self.queue.get(timeout=60)

                if self.handler_function is not None:
                    processed = self.handler_function(app, collection_name, entity)

                    if processed:
                        count_processed += 1
                        logger.debug('Processed [%sth] app/collection/name = %s / %s / %s' % (
                            count_processed, app, collection_name, entity.get('uuid')))

            except KeyboardInterrupt, e:
                raise e

            except Empty:
                logger.warning('EMPTY! Count=%s' % empty_count)

                empty_count += 1
                if empty_count < 10:
                    keep_going = False


def migrate_connections(app, collection_name, source_entity, attempts=0):
    attempts += 1
    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
    target_app = config.get('app_mapping', {}).get(app, app)
    target_collection = config.get('collection_mapping', {}).get(collection_name, collection_name)
    source_uuid = source_entity.get('uuid')

    try:
        connections = source_entity.get('metadata', {}).get('connections', {})

        for connection_name in connections:
            logger.info(
                'Processing connections [%s] of entity [%s/%s]' % (connection_name, collection_name, source_uuid))
            connection_query_url = connection_query_url_template.format(
                org=config.get('org'),
                app=app,
                verb=connection_name,
                collection=target_collection,
                uuid=source_uuid,
                **config.get('source_endpoint'))

            logger.info('QUERY: ' + connection_query_url)

            connection_query = UsergridQuery(connection_query_url)

            connection_stack = []

            for e_connection in connection_query:
                logger.info('Connecting entity [%s/%s] --[%s]--> [%s/%s]' % (
                    collection_name, source_uuid, connection_name, e_connection.get('type'),
                    e_connection.get('uuid')))

                connection_stack.append(e_connection)

            while len(connection_stack) > 0:

                e_connection = connection_stack.pop()

                create_connection_url = connection_create_url_template.format(
                    org=target_org,
                    app=target_app,
                    verb=connection_name,
                    collection=target_collection,
                    uuid=source_uuid,
                    target_uuid=e_connection.get('uuid'),
                    **config.get('target_endpoint'))

                logger.info('CREATE: ' + create_connection_url)
                attempts = 0

                while attempts < 5:
                    attempts += 1

                    r_create = requests.post(create_connection_url)

                    if r_create.status_code == 200:
                        break

                    elif r_create.status_code == 401:
                        logger.warning(
                            'FAILED to create connection at URL=[%s]: %s' % (create_connection_url, r_create.text))
                        logger.warning('WILL Retry')

                        time.sleep(10)

        return True

    except:
        print traceback.format_exc()
        print 'error on entity: \n %s' % json.dumps(source_entity, indent=2)

    return True


def migrate_user_credentials(app, collection_name, source_entity, attempts=0):
    if collection_name != 'users':
        return False

    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
    target_app = config.get('app_mapping', {}).get(app, app)

    source_url = user_credentials_url_template.format(org=config.get('org'),
                                                      app=app,
                                                      uuid=source_entity.get('uuid'),
                                                      **config.get('source_endpoint'))

    target_url = user_credentials_url_template.format(org=target_org,
                                                      app=target_app,
                                                      uuid=source_entity.get('uuid'),
                                                      **config.get('target_endpoint'))

    r = requests.get(source_url, auth=HTTPBasicAuth(config.get('su_username'), config.get('su_password')))

    if r.status_code == 404:
        delete_url = put_entity_url_template.format(org=target_org,
                                                    app=target_app,
                                                    collection=collection_name,
                                                    uuid=source_entity.get('uuid'),
                                                    **config.get('target_endpoint'))

        logger.warning('Deleting corrupted user in target: [%s]' % delete_url)

        r_delete = requests.delete(delete_url)

        if r_delete.status_code != 200:
            logger.warning('Unable to delete corrupted user: HTTP [%s] on URL [%s]: %s' % (
                r_delete.status_code, delete_url, r_delete.text))

        return False

    if r.status_code != 200:
        logger.critical(
            'Unable to migrate credentials due to HTTP [%s] on GET URL [%s]: %s' % (r.status_code, source_url, r.text))
        return False

    source_credentials = r.json()

    logger.info('Putting credentials to [%s]...' % target_url)

    r = requests.put(target_url,
                     data=json.dumps(source_credentials),
                     auth=HTTPBasicAuth(config.get('su_username'), config.get('su_password')))

    if r.status_code != 200:
        logger.critical(
            'Unable to migrate credentials due to HTTP [%s] on PUT URL [%s]: %s' % (r.status_code, target_url, r.text))
        return False

    # logger.info(r.text)
    return True


def migrate_data(app, collection_name, source_entity, attempts=0):
    response = False

    attempts += 1

    if 'metadata' in source_entity: source_entity.pop('metadata')

    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
    target_app = config.get('app_mapping', {}).get(app, app)
    target_collection = config.get('collection_mapping', {}).get(collection_name, collection_name)

    try:
        target_entity_url = put_entity_url_template.format(org=target_org,
                                                           app=target_app,
                                                           collection=target_collection,
                                                           uuid=source_entity.get('uuid'),
                                                           **config.get('target_endpoint'))

        r = requests.put(url=target_entity_url, data=json.dumps(source_entity))

        if r.status_code == 200:
            response = True

        elif r.status_code in ['401', '404']:

            target_collection_url = collection_url_template.format(org=target_org,
                                                                   app=target_app,
                                                                   collection=target_collection,
                                                                   **config.get('target_endpoint'))

            r = requests.post(target_collection_url, json.dumps(source_entity))

            if r.status_code >= 500:

                if attempts <= 3:
                    logger.warn('[%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                        r.status_code, attempts, target_collection_url, json.dumps(source_entity), r.json()))
                    logger.warn('Sleeping before retry...')
                    time.sleep(5)
                    return migrate_data(app, collection_name, source_entity, attempts)

                else:
                    logger.critical('[%s] on attempt [%s] to POST url=[%s], entity=[%s] response=[%s]' % (
                        r.status_code, attempts, target_collection_url, json.dumps(source_entity), r.json()))
                    logger.critical('WILL NOT RETRY')

        elif r.status_code >= 500:

            if attempts <= 3:
                logger.warn('[%s] on attempt [%s] to PUT url=[%s], entity=[%s] response=[%s]' % (
                    r.status_code, attempts, target_entity_url, json.dumps(source_entity), r.json()))
                logger.warn('Sleeping before retry...')
                time.sleep(5)
                return migrate_data(app, collection_name, source_entity, attempts)

            else:
                logger.critical('[%s] on attempt [%s] to PUT url=[%s], entity=[%s] response=[%s]' % (
                    r.status_code, attempts, target_entity_url, json.dumps(source_entity), r.json()))
                logger.critical('WILL NOT RETRY')

        logger.info('create_new | success=[%s] | app/collection/name = %s / %s / %s' % (
            response, app, collection_name, source_entity.get('uuid')))

    except:
        print traceback.format_exc()
        print 'error on entity: \n %s' % json.dumps(source_entity, indent=2)

    return response and migrate_user_credentials(app, collection_name, source_entity, attempts=0)


def parse_args():
    parser = argparse.ArgumentParser(description='Usergrid Org/App Migrator')

    parser.add_argument('-o', '--org',
                        help='Name of the org to migrate',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='Name of one or more apps to include, specify none to include all apps',
                        required=True,
                        action='append')

    parser.add_argument('-c', '--collection',
                        help='Name of one or more collections to include, specify none to include all collections',
                        default=[],
                        action='append')

    parser.add_argument('-m', '--migrate',
                        help='Specifies what to migrate: data, connections, credentials or none (just iterate the apps/collections)',
                        type=str,
                        choices=['data', 'connections', 'credentials', 'none'],
                        default='data')

    parser.add_argument('-s', '--source_config',
                        help='The path to the source endpoint/org configuration file',
                        type=str,
                        default='source.json')

    parser.add_argument('-d', '--target_config',
                        help='The path to the target endpoint/org configuration file',
                        type=str,
                        default='destination.json')

    parser.add_argument('-w', '--workers',
                        help='The number of worker processes to do the migration',
                        type=int,
                        default=16)

    parser.add_argument('--ql',
                        help='The QL to use in the filter for reading data from collections',
                        type=str,
                        default='select *')

    parser.add_argument('--su_username',
                        help='Superuser username',
                        required=False,
                        type=str)

    parser.add_argument('--su_password',
                        help='Superuser Password',
                        required=False,
                        type=str)

    parser.add_argument('--map_app',
                        help="Multiple allowed: A colon-separated string such as 'apples:oranges' which indicates to put data from the app named 'apples' from the source endpoint into app named 'oranges' in the target endpoint",
                        default=[],
                        action='append')

    parser.add_argument('--map_collection',
                        help="One or more colon-separated string such as 'cats:dogs' which indicates to put data from collections named 'cats' from the source endpoint into a collection named 'dogs' in the target endpoint, applicable globally to all apps",
                        default=[],
                        action='append')

    parser.add_argument('--map_org',
                        help="One or more colon-separated strings such as 'red:blue' which indicates to put data from org named 'red' from the source endpoint into a collection named 'blue' in the target endpoint",
                        default=[],
                        action='append')

    my_args = parser.parse_args(sys.argv[1:])

    return vars(my_args)


def init():
    global config

    if config.get('migrate') == 'credentials':

        if config.get('su_password') is None or config.get('su_username') is None:
            message = 'In order to migrate credentials, Superuser parameters (su_password, su_username) are required'
            print message
            logger.critical(message)
            exit()

    config['collection_mapping'] = {}
    config['app_mapping'] = {}
    config['org_mapping'] = {}

    for mapping in config.get('map_collection', []):
        parts = mapping.split(':')

        if len(parts) == 2:
            config['collection_mapping'][parts[0]] = parts[1]
        else:
            logger.warning('Skipping Collection mapping: [%s]' % mapping)

    for mapping in config.get('map_app', []):
        parts = mapping.split(':')

        if len(parts) == 2:
            config['app_mapping'][parts[0]] = parts[1]
        else:
            logger.warning('Skipping App mapping: [%s]' % mapping)

    for mapping in config.get('map_org', []):
        parts = mapping.split(':')

        if len(parts) == 2:
            config['org_mapping'][parts[0]] = parts[1]
        else:
            logger.warning('Skipping Org mapping: [%s]' % mapping)

    with open(config.get('source_config'), 'r') as f:
        config['source_config'] = json.load(f)

    with open(config.get('target_config'), 'r') as f:
        config['target_config'] = json.load(f)

    config['source_endpoint'] = config['source_config'].get('endpoint').copy()
    config['source_endpoint'].update(config['source_config']['credentials'][config['org']])

    target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))

    config['target_endpoint'] = config['target_config'].get('endpoint').copy()
    config['target_endpoint'].update(config['target_config']['credentials'][target_org])


def wait_for(threads, sleep_time=3000):
    wait = True

    while wait:
        wait = False

        for t in threads:

            if t.is_alive():
                wait = True
                time.sleep(sleep_time)
                break


def main():
    global config

    config = parse_args()
    init()

    init_logging()

    # list the apps for the SOURCE org
    source_org_mgmt_url = org_management_url_template.format(org=config.get('org'),
                                                             **config.get('source_endpoint'))
    logger.info('GET %s' % source_org_mgmt_url)
    r = requests.get(source_org_mgmt_url)

    check_response_status(r, source_org_mgmt_url)

    print json.dumps(r.json(), indent=2)

    org_apps = r.json().get('data')

    queue = Queue()
    logger.info('Starting workers...')

    # Check the specified configuration for what to migrate - Data, Connections or App User Credentials
    if config.get('migrate') == 'connections':
        operation = migrate_connections
    elif config.get('migrate') == 'data':
        operation = migrate_data
    elif config.get('migrate') == 'credentials':
        operation = migrate_user_credentials
    else:
        operation = None

    # start the worker processes which will do the work of migrating
    workers = [Worker(queue, operation) for x in xrange(config.get('workers'))]
    [w.start() for w in workers]

    apps_to_process = config.get('app')
    collections_to_process = config.get('collection')

    # iterate the apps retrieved from the org
    for org_app, app_uuid in org_apps.iteritems():
        parts = org_app.split('/')
        app = parts[1]

        # if apps are specified and the current app is not in the list, skip it
        if len(apps_to_process) > 0 and app not in apps_to_process:
            logger.warning('Skipping app=[%s]' % app)
            continue

        logger.info('Processing app=[%s]' % app)

        # it is possible to map source orgs and apps to differently named targets.  This gets the
        # target names for each
        target_org = config.get('org_mapping', {}).get(config.get('org'), config.get('org'))
        target_app = config.get('app_mapping', {}).get(app, app)

        # Check that the target Org/App exists.  If not, move on to the next
        target_app_url = app_url_template.format(org=target_org,
                                                 app=target_app,
                                                 **config.get('target_endpoint'))
        logger.info('GET %s' % target_app_url)
        r_target_apps = requests.get(target_app_url)

        if r_target_apps.status_code != 200:
            logger.error('Target application does not exist at URL=%s' % target_app_url)
            continue

        # get the list of collections from the source org/app
        source_app_url = app_url_template.format(org=config.get('org'),
                                                 app=app,
                                                 **config.get('source_endpoint'))
        logger.info('GET %s' % source_app_url)

        r_collections = requests.get(source_app_url)

        # sometimes this call was not working so I put it in a loop to force it...
        while r_collections.status_code != 200:
            logger.warning('FAILED: GET (%s) [%s] URL: %s' % r_collections.elapsed, r_collections.status_code,
                           source_app_url)
            time.sleep(5)
            r_collections = requests.get(source_app_url)

        app_response = r_collections.json()

        logger.info('App Response: ' + json.dumps(app_response))

        app_entities = app_response.get('entities')

        if len(app_entities) > 0:
            app_entity = app_entities[0]
            collections = app_entity.get('metadata', {}).get('collections', {})
            logger.info('Collection List: %s' % collections)

            # iterate the collections which are returned.
            for collection_name, collection_data in collections.iteritems():

                # filter out collections as configured...
                if collection_name in ignore_collections \
                        or (len(collections_to_process) > 0 and collection_name not in collections_to_process) \
                        or (config.get('migrate') == 'credentials' and collection_name != 'users'):
                    logger.warning('Skipping collection=[%s]' % collection_name)
                    continue

                logger.info('Processing collection=%s' % collection_name)
                counter = 0

                source_collection_url = collection_query_url_template.format(org=config.get('org'),
                                                                             app=app,
                                                                             collection=collection_name,
                                                                             ql=config.get('ql'),
                                                                             **config.get('source_endpoint'))

                # use the UsergridQuery from the Python SDK to iterate the collection
                q = UsergridQuery(source_collection_url)

                try:
                    for entity in q:
                        counter += 1
                        queue.put((app, collection_name, entity))

                except KeyboardInterrupt:
                    logger.warning('Keyboard Interrupt, aborting...')
                    [w.terminate() for w in workers]

        logger.info('DONE!')

    wait_for(workers)

    logger.info('workers DONE!')


def check_response_status(r, url, exit_on_error=True):
    if r.status_code != 200:
        logger.critical('HTTP [%s] on URL=[%s]' % (r.status_code, url))
        logger.critical('Response: %s' % r.text)

        if exit_on_error:
            exit()


main()
